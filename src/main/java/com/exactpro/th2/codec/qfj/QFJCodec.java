package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.value.ValueUtils;
import com.google.protobuf.ByteString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.DataDictionary;
import quickfix.Field;
import quickfix.FieldMap;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.InvalidMessage;
import quickfix.field.BeginString;
import quickfix.field.CheckSum;
import quickfix.field.MsgType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toJson;


public class QFJCodec implements IPipelineCodec {

    private static final Logger LOGGER = LoggerFactory.getLogger(QFJCodec.class);
    private static final String PROTOCOL = "FIX";
    private static final String FIXT11 = "FIXT.1.1";

    private final IPipelineCodecSettings settings;
    private final DataDictionary dataDictionary;
    private final DataDictionary transportDataDictionary;
    private final DataDictionary appDataDictionary;
    private DataDictionary localDataDictionary;

    public QFJCodec(IPipelineCodecSettings settings, DataDictionary dataDictionary, DataDictionary transportDataDictionary, DataDictionary appDataDictionary) {
        this.settings = settings;
        this.dataDictionary = dataDictionary;
        this.transportDataDictionary = transportDataDictionary;
        this.appDataDictionary = appDataDictionary;
    }

    @Override
    public @NotNull MessageGroup encode(@NotNull MessageGroup messageGroup) {

        var messages = messageGroup.getMessagesList();

        if (messages.isEmpty()) {
            return messageGroup;
        }
        return MessageGroup.newBuilder().addAllMessages(messages.stream()
                .map(anyMsg -> {
                            var protocol = anyMsg.getMessage().getMetadata().getProtocol();

                            if (anyMsg.hasMessage() && (protocol.isEmpty() || protocol.equals(PROTOCOL))) {
                                return AnyMessage.newBuilder().setRawMessage(encodeMessage(anyMsg.getMessage())).build();
                            } else {
                                return anyMsg;
                            }
                        }
                )
                .collect(Collectors.toSet())
        ).build();
    }

    public RawMessage encodeMessage(Message message) {

        String msgName = message.getMetadata().getMessageType();
        quickfix.Message fixMessage = getFixMessage(message.getFieldsMap(), msgName);

        byte[] strFixMessage = fixMessage.toString().getBytes();
        var msgMetadata = message.getMetadata();

        return RawMessage.newBuilder()
                .setParentEventId(message.getParentEventId())
                .setBody(ByteString
                        .copyFrom(strFixMessage))
                .setMetadata(RawMessageMetadata.newBuilder()
                        .putAllProperties(msgMetadata.getPropertiesMap())
                        .setProtocol(PROTOCOL)
                        .setId(msgMetadata.getId())
                        .setTimestamp(msgMetadata.getTimestamp())
                        .build())
                .build();
    }

    private quickfix.Message getFixMessage(Map<String, Value> fieldsMap, String msgName) {

        boolean isFix50 = false;
        if (appDataDictionary != null) {
            isFix50 = true;
            localDataDictionary = appDataDictionary;
        } else if (dataDictionary != null) {
            localDataDictionary = dataDictionary;
        } else {
            throw new IllegalStateException("No available dictionaries.");
        }

        String msgType = localDataDictionary.getMsgType(msgName);

        quickfix.Message message = new quickfix.Message();
        quickfix.Message.Header header = message.getHeader();
        quickfix.Message.Trailer trailer = message.getTrailer();

        if (!fieldsMap.containsKey("Header")) {
            header.setField(new BeginString(localDataDictionary.getVersion()));
        }

        String key;
        int tag;
        Value value;

        for (Map.Entry<String, Value> item : fieldsMap.entrySet()) {

            key = item.getKey();
            value = item.getValue();

            if (key.equals("Header")) {
                setHeader(value, header, isFix50);
            } else if (key.equals("Trailer")) {
                setTrailer(value, trailer, isFix50);
            } else {

                tag = validateTag(localDataDictionary.getFieldTag(key), key);

                logTagValue(key, value.getSimpleValue(), tag);

                if (isGroup(value)) {
                    for (Group group : getGroups(value, tag, localDataDictionary)) {
                        message.addGroup(group);
                    }
                } else {
                    if (isHeaderField(tag, isFix50)) {
                        throw new IllegalArgumentException("Header tag \"" + key + "\" in the body");
                    } else if (isTrailerField(tag, isFix50)) {
                        throw new IllegalArgumentException("Trailer tag \"" + key + "\" in the body");
                    } else {
                        if (!localDataDictionary.isMsgField(msgType, tag)) {
                            throw new IllegalArgumentException("tag \"" + tag + "\" does not belong to this type of message: " + msgType);
                        }
                        Field<?> field = new Field<>(tag, value.getSimpleValue());
                        message.setField(tag, field);
                    }
                }
            }
        }
        header.setField(new MsgType(msgType));

        return message;
    }

    private List<Group> getGroups(Value value, int tag, DataDictionary dataDictionary) {

        List<Value> valuesList = value.getListValue().getValuesList();
        List<Group> groups = new ArrayList<>();

        for (Value innerValue : valuesList) {
            Group group = null;
            String innerKey;
            int innerTag;
            Value fieldsMapValue;

            for (Map.Entry<String, Value> fieldsMap : innerValue.getMessageValue().getFieldsMap().entrySet()) {

                innerKey = fieldsMap.getKey();
                innerTag = validateTag(dataDictionary.getFieldTag(innerKey), innerKey);
                fieldsMapValue = fieldsMap.getValue();

                logTagValue(innerKey, innerValue.getSimpleValue(), innerTag);

                if (group == null) {
                    group = new Group(tag, innerTag);
                }
                if (isGroup(fieldsMapValue)) {
                    List<Group> innerGroups = getGroups(fieldsMapValue, innerTag, dataDictionary);
                    for (Group innerGroup : innerGroups) {
                        group.addGroup(innerGroup);
                    }
                } else {
                    Field<?> groupField = new Field<>(innerTag, fieldsMapValue.getSimpleValue());
                    group.setField(innerTag, groupField);
                }
            }
            groups.add(group);
        }
        return groups;
    }

    private void setTrailer(Value value, quickfix.Message.Trailer trailer, boolean isFix50) {
        setFieldMap(value, trailer, isFix50, false);
    }

    private void setHeader(Value value, quickfix.Message.Header header, boolean isFix50) {
        setFieldMap(value, header, isFix50, true);
    }

    private void setFieldMap(Value value, FieldMap fieldMap, boolean isFix50, boolean isHeader) {

        if (!isValidMessageValue(value)) {
            if (isHeader) {
                LOGGER.error("Header is empty or contain incorrect values");
            } else {
                LOGGER.error("Trailer is empty or contain incorrect values");
            }
        }
        Map<String, Value> fieldsMap = value.getMessageValue().getFieldsMap();

        String key;
        int tag;
        Value innerValue;

        for (Map.Entry<String, Value> fieldsMapEntry : fieldsMap.entrySet()) {

            key = fieldsMapEntry.getKey();
            tag = validateTag(localDataDictionary.getFieldTag(key), key);
            innerValue = fieldsMapEntry.getValue();

            logTagValue(key, value.getSimpleValue(), tag);

            if (isGroup(innerValue)) {
                for (Group group : getGroups(innerValue, tag, localDataDictionary)) {
                    fieldMap.addGroup(group);
                }
            } else {
                if (isHeader && !isHeaderField(tag, isFix50)) {
                    throw new IllegalArgumentException("Not header tag in Header directory: " + key);
                } else if (!isHeader && !isTrailerField(tag, isFix50)) {
                    throw new IllegalArgumentException("Not trailer tag in Trailer directory: " + key);
                }

                Field<?> field = new Field<>(tag, innerValue.getSimpleValue());
                fieldMap.setField(tag, field);
            }
        }
    }

    private boolean isGroup(Value value) {
        return value.getListValue().getValuesCount() > 0;
    }

    private boolean isValidMessageValue(Value value) {
        return value.getMessageValue().getFieldsCount() > 0;
    }

    private boolean isHeaderField(int tag, boolean isFix50) {
        if (isFix50) {
            return transportDataDictionary.isHeaderField(tag);
        } else {
            return dataDictionary.isHeaderField(tag);
        }
    }

    private boolean isTrailerField(int tag, boolean isFix50) {
        if (isFix50) {
            return transportDataDictionary.isTrailerField(tag);
        } else {
            return dataDictionary.isTrailerField(tag);
        }
    }

    @Override
    public @NotNull MessageGroup decode(@NotNull MessageGroup messageGroup) {

        var messages = messageGroup.getMessagesList();

        if (messages.isEmpty() || messages.stream().allMatch(AnyMessage::hasMessage)) {
            return messageGroup;
        }

        return MessageGroup.newBuilder().addAllMessages(messages.stream()
                .map(anyMsg -> {
                            if (anyMsg.hasRawMessage()) {
                                try {
                                    return AnyMessage.newBuilder().setMessage(decodeMessage(anyMsg.getRawMessage())).build();
                                } catch (Exception e) {
                                    throw new IllegalStateException("Cannot decode message " + toJson(anyMsg.getRawMessage()));
                                }
                            } else {
                                return anyMsg;
                            }
                        }
                )
                .collect(Collectors.toSet())
        ).build();
    }

    public Message decodeMessage(RawMessage rawMessage) throws InvalidMessage {

        String strMessage = new String(rawMessage.getBody().toByteArray(), StandardCharsets.UTF_8);
        quickfix.Message qfjMessage = new quickfix.Message();

        boolean isFix50 = false;
        if (appDataDictionary != null) {
            qfjMessage.fromString(strMessage, transportDataDictionary, appDataDictionary, true, true);
            localDataDictionary = appDataDictionary;
            isFix50 = true;
        } else if (dataDictionary != null) {
            qfjMessage.fromString(strMessage, dataDictionary, true, true);
            localDataDictionary = dataDictionary;
        } else {
            throw new IllegalStateException("No available dictionaries.");
        }

        Map<String, Value> fieldsMap = new TreeMap<>();

        String msgType = "";
        try {
            msgType = qfjMessage.getHeader().getString(MsgType.FIELD);
        } catch (FieldNotFound fieldNotFound) {
            LOGGER.error("Cannot find message type in message: {}", qfjMessage);
        }

        Iterator<Field<?>> headerIterator = qfjMessage.getHeader().iterator();
        Message header = getMessage(headerIterator, Message.newBuilder(), qfjMessage.getHeader(), isFix50);
        fieldsMap.put("Header", ValueUtils.toValue(header));

        Iterator<Field<?>> iterator = qfjMessage.iterator();
        fillMessageBody(iterator, fieldsMap, qfjMessage, msgType);

        Iterator<Field<?>> trailerIterator = qfjMessage.getTrailer().iterator();
        Message trailer = getMessage(trailerIterator, Message.newBuilder(), strMessage);
        fieldsMap.put("Trailer", ValueUtils.toValue(trailer));


        String msgName = localDataDictionary.getValueName(MsgType.FIELD, msgType);
        return Message.newBuilder()
                .putAllFields(fieldsMap)
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

    private void fillMessageBody(Iterator<Field<?>> iterator, Map<String, Value> fieldsMap, quickfix.Message qfjMessage, String msgType) {
        while (iterator.hasNext()) {
            Field<?> field = iterator.next();

            if (localDataDictionary.isGroup(msgType, field.getTag())) {
                List<Group> groups = qfjMessage.getGroups(field.getTag());
                ListValue.@NotNull Builder listValue = ValueUtils.listValue();
                DataDictionary innerDataDictionary = localDataDictionary.getGroup(msgType, field.getTag()).getDataDictionary();

                fillListValue(listValue, innerDataDictionary, groups, msgType);
                fieldsMap.put(localDataDictionary.getFieldName(field.getTag()), ValueUtils.toValue(listValue));
            } else {
                putField(localDataDictionary, fieldsMap, field);
            }
        }

    }

    private void fillListValue(ListValue.Builder listValue, DataDictionary dataDictionary, List<Group> groups, String msgType) {
        for (Group group : groups) {
            Message.Builder messageBuilder = Message.newBuilder();
            Iterator<Field<?>> innerIterator = group.iterator();
            Message innerMessage = getMessage(innerIterator, messageBuilder, dataDictionary, group, msgType);
            listValue.addValues(ValueUtils.toValue(innerMessage));
        }
    }

    //ADDING BODY TO MESSAGE
    private Message getMessage(Iterator<Field<?>> iterator, Message.Builder messageBuilder, DataDictionary dataDictionary, Group group, String msgType) {

        while (iterator.hasNext()) {
            Field<?> field = iterator.next();

            if (dataDictionary.isGroup(msgType, field.getTag())) {
                ListValue.@NotNull Builder listValue = ValueUtils.listValue();
                List<Group> groups = group.getGroups(field.getTag());
                DataDictionary.GroupInfo groupInfo = Objects.requireNonNull(dataDictionary.getGroup(msgType, field.getTag()),
                        () -> "No GroupInfo for this combination of tag:{}" + field.getTag() + " and msgType:{}" + msgType);
                DataDictionary innerDataDictionary = groupInfo.getDataDictionary();

                fillListValue(listValue, innerDataDictionary, groups, msgType);
                messageBuilder.putFields(localDataDictionary.getFieldName(field.getTag()), Value.newBuilder().setListValue(listValue).build());
            } else {
                putMessageField(messageBuilder, localDataDictionary, field);
            }
        }
        return messageBuilder.build();
    }

    //ADDING HEADER TO MESSAGE
    private Message getMessage(Iterator<Field<?>> iterator, Message.Builder messageBuilder, quickfix.Message.Header header, boolean isFix50) {

        DataDictionary dataDictionary = isFix50 ? transportDataDictionary : localDataDictionary;

        while (iterator.hasNext()) {
            Field<?> field = iterator.next();

            if (dataDictionary.isHeaderGroup(field.getTag())) {
                ListValue.@NotNull Builder listValue = ValueUtils.listValue();
                List<Group> groups = header.getGroups(field.getTag());

                DataDictionary.GroupInfo groupInfo = Objects.requireNonNull(dataDictionary.getGroup(DataDictionary.HEADER_ID, field.getTag()),
                        () -> "No GroupInfo for this combination of tag:{}" + field.getTag() + " and msgType:{}" + DataDictionary.HEADER_ID);
                DataDictionary innerDataDictionary = groupInfo.getDataDictionary();

                fillListValue(listValue, innerDataDictionary, groups, DataDictionary.HEADER_ID);
                messageBuilder.putFields(dataDictionary.getFieldName(field.getTag()), Value.newBuilder().setListValue(listValue).build());
            } else {
                putMessageField(messageBuilder, dataDictionary, field);
            }
        }
        return messageBuilder.build();
    }

    //ADDING TRAILER TO MESSAGE
    private Message getMessage(Iterator<Field<?>> iterator, Message.Builder messageBuilder, String strQfjMessage) {

        while (iterator.hasNext()) {
            Field<?> field = iterator.next();
            if (field.getTag() == CheckSum.FIELD) {
                Field<?> checksumField = new Field<>(CheckSum.FIELD, getChecksum(strQfjMessage));
                putMessageField(messageBuilder, localDataDictionary, checksumField);
            } else {
                putMessageField(messageBuilder, localDataDictionary, field);
            }
        }
        return messageBuilder.build();
    }

    private void putField(DataDictionary dataDictionary, Map<String, Value> fieldsMap, Field<?> field) {
        fieldsMap.put(dataDictionary.getFieldName(field.getTag()), ValueUtils.toValue(field.getObject()));
    }

    private void putMessageField(Message.Builder messageBuilder, DataDictionary dataDictionary, Field<?> field) {
        messageBuilder.putFields(dataDictionary.getFieldName(field.getTag()), ValueUtils.toValue(field.getObject()));
    }

    @Override
    public void close() {
    }

    private int validateTag(int tag, String key) {
        if (tag == -1) {
            throw new IllegalStateException("No such tag in dictionary for tag name: " + key);
        }
        return tag;
    }

    private String getChecksum(String qfjMessage) {
        return qfjMessage.substring(qfjMessage.lastIndexOf("\00110=") + 4, qfjMessage.lastIndexOf('\001'));
    }

    private void logTagValue(String key, String simpleValue, int tag) {
        LOGGER.trace("key: {}", key);
        LOGGER.trace("value: {}", simpleValue);
        LOGGER.trace("TAG №: {}", tag);
    }
}