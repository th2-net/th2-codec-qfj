/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.value.ValueUtils;
import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;

import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quickfix.DataDictionary;
import quickfix.Field;
import quickfix.FieldMap;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.InvalidMessage;
import quickfix.field.BeginString;
import quickfix.field.MsgType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.exactpro.th2.common.message.MessageUtils.toJson;

@AutoService(IPipelineCodec.class)
public class QFJCodec implements IPipelineCodec {
    private static Logger LOGGER = LoggerFactory.getLogger(QFJCodec.class);

    public static final String PROTOCOL = "FIX";
    public static final String HEADER = "header";
    public static final String TRAILER = "trailer";

    private final DataDictionary transportDataDictionary;
    private final DataDictionary appDataDictionary;

    public QFJCodec(QFJCodecSettings qfjCodecSettings, @Nullable DataDictionary dataDictionary, @Nullable DataDictionary transportDataDictionary, @Nullable DataDictionary appDataDictionary) {

        if (appDataDictionary != null) {
            this.appDataDictionary = configureDictionary(qfjCodecSettings, appDataDictionary);
            this.transportDataDictionary = configureDictionary(qfjCodecSettings, transportDataDictionary);
        } else if (dataDictionary != null) {
            configureDictionary(qfjCodecSettings, dataDictionary);
            this.appDataDictionary = dataDictionary;
            this.transportDataDictionary = dataDictionary;
        } else {
            throw new IllegalStateException("No available dictionaries.");
        }
    }

    private DataDictionary configureDictionary(QFJCodecSettings settings, DataDictionary dictionary) {
        dictionary.setCheckFieldsOutOfOrder(settings.isCheckFieldsOutOfOrder());
        return dictionary;
    }

    @Override
    public @NotNull MessageGroup encode(@NotNull MessageGroup messageGroup) {

        var messages = messageGroup.getMessagesList();

        if (messages.isEmpty()) {
            return messageGroup;
        }
        MessageGroup.Builder msgBuilder = MessageGroup.newBuilder();
        messages.forEach(anyMsg -> {
                    var protocol = anyMsg.getMessage().getMetadata().getProtocol();

                    if (anyMsg.hasMessage() && (protocol.isEmpty() || protocol.equals(PROTOCOL))) {
                        msgBuilder.addMessages(AnyMessage.newBuilder()
                                .setRawMessage(encodeMessage(anyMsg.getMessage())).build());
                    } else {
                        msgBuilder.addMessages(anyMsg);
                    }
                }
        );
        return msgBuilder.build();
    }


    public RawMessage encodeMessage(Message message) {

        String msgName = message.getMetadata().getMessageType();
        String msgType = transportDataDictionary.getMsgType(msgName) != null ? transportDataDictionary.getMsgType(msgName) : appDataDictionary.getMsgType(msgName);
        if (msgType == null){
            throw new IllegalStateException("No such message type for message name: " + msgName);
        }
        quickfix.Message fixMessage = getFixMessage(message.getFieldsMap(), msgType);

        byte[] strFixMessage = fixMessage.toString().getBytes();
        var msgMetadata = message.getMetadata();

        RawMessage.Builder rawBuilder = RawMessage.newBuilder();
        if (!message.getParentEventId().getId().equals("")) {
            rawBuilder.setParentEventId(message.getParentEventId());
        }
        rawBuilder.setBody(ByteString
                        .copyFrom(strFixMessage))
                .setMetadata(RawMessageMetadata.newBuilder()
                        .putAllProperties(msgMetadata.getPropertiesMap())
                        .setProtocol(PROTOCOL)
                        .setId(msgMetadata.getId())
                        .setTimestamp(msgMetadata.getTimestamp())
                        .build());
        return rawBuilder.build();
    }

    private quickfix.Message getFixMessage(Map<String, Value> fieldsMap, String msgType) {

        quickfix.Message message = new quickfix.Message();

        if (!fieldsMap.containsKey(HEADER)) {
            message.getHeader().setField(new BeginString(transportDataDictionary.getVersion()));
            message.getHeader().setField(new MsgType(msgType));
        }
        setFields(fieldsMap, message, appDataDictionary, msgType);
        return message;
    }

    private void setFields(Map<String, Value> fieldsMap, quickfix.FieldMap qfjFieldMap, DataDictionary dataDictionary, String msgType) {

        fieldsMap.forEach((key, value) -> {
            if (key.equals(HEADER)) {
                quickfix.Message message = (quickfix.Message) qfjFieldMap;
                setFields(value.getMessageValue().getFieldsMap(), message.getHeader(), transportDataDictionary, DataDictionary.HEADER_ID);
            } else if (key.equals(TRAILER)) {
                quickfix.Message message = (quickfix.Message) qfjFieldMap;
                setFields(value.getMessageValue().getFieldsMap(), message.getTrailer(), transportDataDictionary, DataDictionary.TRAILER_ID);
            } else {

                int tag = validateTag(dataDictionary.getFieldTag(key), key, dataDictionary.getFullVersion());

                if (dataDictionary.isGroup(msgType, tag)) {
                    DataDictionary.GroupInfo groupInfo = dataDictionary.getGroup(msgType, tag);
                    List<Group> groups = getGroups(value.getListValue().getValuesList(), tag, groupInfo.getDelimiterField(),
                            dataDictionary, groupInfo.getDataDictionary(), msgType);
                    groups.forEach(qfjFieldMap::addGroup);
                } else {
                    if (!dataDictionary.isMsgField(msgType, tag)) {
                        throw new IllegalArgumentException("Tag \"" + key + "\" does not belong to this type of message: " + msgType);
                    }
                    Field<?> field = new Field<>(tag, value.getSimpleValue());
                    qfjFieldMap.setField(tag, field);
                }
            }
        });
    }

    private List<Group> getGroups(List<Value> values, int tag, int delim, DataDictionary dataDictionary, DataDictionary
            groupDictionary, String msgType) {

        List<Group> groups = new ArrayList<>();

        for (Value innerValue : values) {
            Group group = null;
            String innerKey;
            int innerTag;
            Value fieldsMapValue;

            for (Map.Entry<String, Value> fieldsMap : innerValue.getMessageValue().getFieldsMap().entrySet()) {

                if (group == null) {
                    group = new Group(tag, delim);
                }
                innerKey = fieldsMap.getKey();
                innerTag = validateTag(dataDictionary.getFieldTag(innerKey), innerKey, dataDictionary.getFullVersion());
                fieldsMapValue = fieldsMap.getValue();


                if (groupDictionary.isGroup(msgType, innerTag)) {
                    DataDictionary.GroupInfo groupInfo = groupDictionary.getGroup(msgType, innerTag);
                    DataDictionary innerGroupDictionary = groupInfo.getDataDictionary();
                    delim = groupInfo.getDelimiterField();
                    List<Group> innerGroups = getGroups(fieldsMapValue.getListValue().getValuesList(), innerTag, delim, dataDictionary, innerGroupDictionary, msgType);
                    for (Group innerGroup : innerGroups) {
                        group.addGroup(innerGroup);
                    }
                } else {
                    if (!groupDictionary.isField(innerTag)) {
                        throw new IllegalArgumentException("Invalid tag " + innerKey + " for message group " + dataDictionary.getFieldName(tag));
                    }
                    Field<?> groupField = new Field<>(innerTag, fieldsMapValue.getSimpleValue());
                    group.setField(innerTag, groupField);
                }
            }
            groups.add(group);
        }
        return groups;
    }

    @Override
    public @NotNull MessageGroup decode(@NotNull MessageGroup messageGroup) {

        var messages = messageGroup.getMessagesList();

        if (messages.isEmpty() || messages.stream().allMatch(AnyMessage::hasMessage)) {
            return messageGroup;
        }

        MessageGroup.Builder msgBuilder = MessageGroup.newBuilder();
        messages.forEach(anyMsg -> {
                    if (anyMsg.hasRawMessage()) {
                        try {
                            msgBuilder.addMessages(AnyMessage.newBuilder()
                                    .setMessage(decodeMessage(anyMsg.getRawMessage())).build());
                        } catch (Exception e) {
                            throw new IllegalStateException("Cannot decode message " + toJson(anyMsg.getRawMessage()), e);
                        }
                    } else {
                        msgBuilder.addMessages(anyMsg);
                    }
                }
        );
        return msgBuilder.build();
    }

    public Message decodeMessage(RawMessage rawMessage) throws InvalidMessage {

        quickfix.Message qfjMessage = parseQfjMessage(rawMessage.getBody().toByteArray());

        String msgType;
        try {
            msgType = qfjMessage.getHeader().getString(MsgType.FIELD);
        } catch (FieldNotFound fieldNotFound) {
            throw new IllegalArgumentException("Cannot find message type in message: " + qfjMessage, fieldNotFound);
        }

        //FIXME: replace to required not null with origin message type in text
        String msgName = ObjectUtils.defaultIfNull(transportDataDictionary.getValueName(MsgType.FIELD, msgType), msgType);
        Message.Builder builder = Message.newBuilder();


        Iterator<Field<?>> headerIterator = qfjMessage.getHeader().iterator();
        Message header = getMessage(headerIterator, transportDataDictionary, qfjMessage.getHeader(), DataDictionary.HEADER_ID);
        builder.putFields(HEADER, ValueUtils.toValue(header));


        Iterator<Field<?>> iterator = qfjMessage.iterator();
        fillMessageBody(iterator, builder, qfjMessage, msgType);


        Iterator<Field<?>> trailerIterator = qfjMessage.getTrailer().iterator();
        Message trailer = getMessage(trailerIterator, transportDataDictionary, qfjMessage.getTrailer(), DataDictionary.TRAILER_ID);
        builder.putFields(TRAILER, ValueUtils.toValue(trailer));

        return builder
                .setParentEventId(rawMessage.getParentEventId())
                .setMetadata(MessageMetadata.newBuilder()
                        .setId(rawMessage.getMetadata().getId())
                        .setTimestamp(rawMessage.getMetadata().getTimestamp())
                        .setProtocol(rawMessage.getMetadata().getProtocol())
                        .setMessageType(msgName)
                        .putAllProperties(rawMessage.getMetadata().getPropertiesMap())
                        .build())
                .build();
    }

    @NotNull
    private quickfix.Message parseQfjMessage(byte[] rawMessage) throws InvalidMessage {
        String strMessage = new String(rawMessage,  StandardCharsets.UTF_8);

        quickfix.Message qfjMessage = new quickfix.Message();

        if (Objects.equals(appDataDictionary, transportDataDictionary)) {
            qfjMessage.fromString(strMessage, appDataDictionary, true, true);
        } else {
            qfjMessage.fromString(strMessage, transportDataDictionary, appDataDictionary, true, true);
        }

        if (qfjMessage.getException() != null) {
            throw new IllegalStateException("Can't decode message '" + strMessage + '\'', qfjMessage.getException());
        }
        return qfjMessage;
    }

    private void fillMessageBody(Iterator<Field<?>> iterator, Message.Builder builder, quickfix.Message qfjMessage, String msgType) {

        DataDictionary dictionary = qfjMessage.isAdmin() ? transportDataDictionary : appDataDictionary;

        iterator.forEachRemaining(field -> {
            if (field == null) {
                LOGGER.warn("Null filed in the message with type {}, qfj msg {}", msgType, qfjMessage);
                return;
            }

            if (dictionary.isGroup(msgType, field.getTag())) {
                List<Group> groups = qfjMessage.getGroups(field.getTag());
                @NotNull ListValue.Builder listValue = ValueUtils.listValue();
                DataDictionary innerDataDictionary = dictionary.getGroup(msgType, field.getTag()).getDataDictionary();

                fillListValue(listValue, innerDataDictionary, groups, msgType);
                builder.putFields(dictionary.getFieldName(field.getTag()), ValueUtils.toValue(listValue));
            } else {
                if (!dictionary.isMsgField(msgType, field.getField())) {
                    throw new IllegalArgumentException("Invalid filed=" + dictionary.getFieldName(field.getField()) + '(' + field.getTag() + ") for message type " + msgType);
                }
                builder.putFields(dictionary.getFieldName(field.getTag()), ValueUtils.toValue(field.getObject()));
            }
        });
    }

    private void fillListValue(ListValue.Builder listValue, DataDictionary
            dataDictionary, List<Group> groups, String msgType) {
        for (Group group : groups) {
            Iterator<Field<?>> innerIterator = group.iterator();
            Message innerMessage = getMessage(innerIterator, dataDictionary, group, msgType);
            listValue.addValues(ValueUtils.toValue(innerMessage));
        }
    }

    private Message getMessage(Iterator<Field<?>> iterator, DataDictionary dataDictionary, FieldMap fieldMap, String msgType) {
        Message.Builder messageBuilder = Message.newBuilder();
        iterator.forEachRemaining(field -> {
            DataDictionary localDataDictionary = transportDataDictionary.isHeaderField(field.getField()) ||
                    transportDataDictionary.isTrailerField(field.getField()) ? transportDataDictionary : appDataDictionary;
            if (dataDictionary.isGroup(msgType, field.getTag())) {
                @NotNull ListValue.Builder listValue = ValueUtils.listValue();
                List<Group> groups = fieldMap.getGroups(field.getTag());

                DataDictionary.GroupInfo groupInfo = Objects.requireNonNull(dataDictionary.getGroup(msgType, field.getTag()),
                        () -> "No GroupInfo for this combination of tag:{}" + field.getTag() + " and msgType:{}" + msgType);

                DataDictionary innerDataDictionary = groupInfo.getDataDictionary();

                fillListValue(listValue, innerDataDictionary, groups, msgType);
                messageBuilder.putFields(localDataDictionary.getFieldName(field.getTag()), Value.newBuilder().setListValue(listValue).build());
            } else {
                if (!fieldMap.isSetField(field.getField())) {
                    throw new IllegalArgumentException("Invalid tag \"" + dataDictionary.getFieldName(field.getField()) + "\" for message group " + fieldMap);
                }
                putMessageField(messageBuilder, localDataDictionary, field);
            }
        });
        return messageBuilder.build();
    }

    private void putMessageField(Message.Builder messageBuilder, DataDictionary dataDictionary, Field<?> field) {
        messageBuilder.putFields(dataDictionary.getFieldName(field.getTag()), ValueUtils.toValue(field.getObject()));
    }

    @Override
    public void close() {
    }

    private int validateTag(int tag, String key, String dictionary) {
        if (tag == -1) {
            throw new IllegalStateException("No such tag in dictionary " + dictionary + " with tag name: " + key);
        }
        return tag;
    }
}