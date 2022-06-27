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
import com.exactpro.th2.codec.api.IReportingContext;
import com.exactpro.th2.codec.api.impl.ReportingContext;
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
import org.apache.commons.collections4.iterators.PushbackIterator;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.DataDictionary;
import quickfix.Field;
import quickfix.FieldConvertError;
import quickfix.FieldMap;
import quickfix.FieldNotFound;
import quickfix.FieldType;
import quickfix.Group;
import quickfix.InvalidMessage;
import quickfix.UtcTimestampPrecision;
import quickfix.field.BeginString;
import quickfix.field.MsgType;
import quickfix.field.converter.BooleanConverter;
import quickfix.field.converter.DecimalConverter;
import quickfix.field.converter.IntConverter;
import quickfix.field.converter.UtcDateOnlyConverter;
import quickfix.field.converter.UtcTimeOnlyConverter;
import quickfix.field.converter.UtcTimestampConverter;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.exactpro.th2.common.message.MessageUtils.toJson;

@AutoService(IPipelineCodec.class)
public class QFJCodec implements IPipelineCodec {
    private static final Logger LOGGER = LoggerFactory.getLogger(QFJCodec.class);

    public static final String PROTOCOL = "FIX";
    public static final String HEADER = "header";
    public static final String TRAILER = "trailer";

    private final DataDictionary transportDataDictionary;
    private final DataDictionary appDataDictionary;
    private final boolean replaceValuesWithEnumNames;
    private final boolean useComponents;

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
        this.replaceValuesWithEnumNames = qfjCodecSettings.isReplaceValuesWithEnumNames();
        this.useComponents = qfjCodecSettings.isUseComponents();
    }

    private DataDictionary configureDictionary(QFJCodecSettings settings, DataDictionary dictionary) {
        dictionary.setCheckFieldsOutOfOrder(settings.isCheckFieldsOutOfOrder());
        return dictionary;
    }

    @Override
    public @NotNull MessageGroup encode(@NotNull MessageGroup messageGroup, @NotNull IReportingContext iReportingContext) {

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
        if (msgType == null) {
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
            } else if (useComponents && dataDictionary.isMsgComponent(msgType, key)) {
                setFields(value.getMessageValue().getFieldsMap(), qfjFieldMap, dataDictionary, msgType);
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
                    Field<?> field = getField(tag, value.getSimpleValue(), dataDictionary);
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

            for (Map.Entry<String, Value> fieldsMap : innerValue.getMessageValue().getFieldsMap().entrySet()) {

                if (group == null) {
                    group = new Group(tag, delim);
                }
                innerKey = fieldsMap.getKey();

                if (useComponents && groupDictionary.isMsgComponent(msgType, innerKey)) {
                    extractComponent(fieldsMap.getValue().getMessageValue().getFieldsMap(), group, dataDictionary, groupDictionary, msgType, tag);
                } else {
                    setGroup(innerKey, group, dataDictionary, groupDictionary, fieldsMap.getValue(), msgType, tag);
                }
            }
            groups.add(group);
        }
        return groups;
    }

    private Field<?> getField(int tag, String valueName, DataDictionary dataDictionary) {

        String value = dataDictionary.getValue(tag, valueName);
        return new Field<>(tag, value == null ? convertToType(dataDictionary.getFieldType(tag), valueName) : value);
    }

    private void extractComponent(Map<String, Value> fieldsMap, Group group, DataDictionary dataDictionary,
                                  DataDictionary groupDictionary, String msgType, int outerTag) {
        fieldsMap.forEach((key, value) -> {
            if (dataDictionary.isMsgComponent(msgType, key)) {
                extractComponent(value.getMessageValue().getFieldsMap(), group, dataDictionary, groupDictionary, msgType, outerTag);
            } else {
                setGroup(key, group, dataDictionary, groupDictionary, value, msgType, outerTag);
            }
        });
    }

    private void setGroup(String key, Group group, DataDictionary dataDictionary, DataDictionary groupDictionary, Value value, String msgType, int tag) {

        var innerTag = validateTag(dataDictionary.getFieldTag(key), key, dataDictionary.getFullVersion());

        if (groupDictionary.isGroup(msgType, innerTag)) {
            DataDictionary.GroupInfo groupInfo = groupDictionary.getGroup(msgType, innerTag);
            DataDictionary innerGroupDictionary = groupInfo.getDataDictionary();
            int delim = groupInfo.getDelimiterField();
            List<Group> innerGroups = getGroups(value.getListValue().getValuesList(), innerTag, delim, dataDictionary, innerGroupDictionary, msgType);
            for (Group innerGroup : innerGroups) {
                group.addGroup(innerGroup);
            }
        } else {
            if (!groupDictionary.isField(innerTag)) {
                throw new IllegalArgumentException("Invalid tag " + key + " for message group " + dataDictionary.getFieldName(tag));
            }
            Field<?> groupField = getField(innerTag, value.getSimpleValue(), dataDictionary);
            group.setField(innerTag, groupField);
        }
    }

    private String decodeFromType(FieldType fieldType, String value) {
        try {
            switch (fieldType) {
            case UTCTIMESTAMP:
                return UtcTimestampConverter.convertToLocalDateTime(value).toString();
            case UTCTIMEONLY:
                return UtcTimeOnlyConverter.convertToLocalTime(value).toString();
            case UTCDATEONLY:
                return UtcDateOnlyConverter.convertToLocalDate(value).toString();
            case FLOAT:
            case AMT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
                return DecimalConverter.convert(value).toPlainString();
            case INT:
            case LENGTH:
            case NUMINGROUP:
            case SEQNUM:
                return String.valueOf(IntConverter.convert(value));
            case BOOLEAN:
                return String.valueOf(BooleanConverter.convert(value));
            default:
                return value;
            }
        } catch (FieldConvertError ex) {
            throw new IllegalArgumentException("cannot convert field " + value + " of type " + fieldType, ex);
        }
    }

    private String convertToType(FieldType fieldType, String value) {
        switch (fieldType) {
            case UTCTIMESTAMP:
                return toTimestamp(fieldType, value);
            case UTCTIMEONLY:
                return toTimeOnly(fieldType, value);
            case UTCDATEONLY:
                return toDateOnly(fieldType, value);
            case FLOAT:
            case AMT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
                return toDecimal(fieldType, value);
            case INT:
            case LENGTH:
            case NUMINGROUP:
            case SEQNUM:
                return toInt(fieldType, value);
            case BOOLEAN:
                return toBool(fieldType, value);
            default:
                return value;
        }
    }

    private String toBool(FieldType fieldType, String value) {
        boolean bool;
        try {
            if (!"true".equals(value) && !"false".equals(value)) {
                bool = BooleanConverter.convert(value);
            } else {
                bool = "true".equals(value);
            }
        } catch (FieldConvertError error) {
            throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
        }
        return BooleanConverter.convert(bool);
    }

    private String toInt(FieldType fieldType, String value) {
        int intValue;
        try {
            intValue = IntConverter.convert(value);
        } catch (FieldConvertError error) {
            throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
        }
        return IntConverter.convert(intValue);
    }

    private String toDecimal(FieldType fieldType, String value) {
        BigDecimal decimal;
        try {
            decimal = DecimalConverter.convert(value);
        } catch (FieldConvertError error) {
            throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
        }
        return DecimalConverter.convert(decimal);
    }

    @NotNull
    private String toTimestamp(FieldType fieldType, String value) {
        LocalDateTime localDateTime;
        try {
            localDateTime = LocalDateTime.parse(value);
        } catch (DateTimeParseException ex) {
            try {
                localDateTime = UtcTimestampConverter.convertToLocalDateTime(value);
            } catch (FieldConvertError error) {
                throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
            }
        }
        return UtcTimestampConverter.convert(localDateTime, calculatePrecision(localDateTime.getNano()));
    }

    @NotNull
    private String toTimeOnly(FieldType fieldType, String value) {
        LocalTime localTime;
        try {
            localTime = LocalTime.parse(value);
        } catch (DateTimeParseException ex) {
            try {
                localTime = UtcTimeOnlyConverter.convertToLocalTime(value);
            } catch (FieldConvertError error) {
                throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
            }
        }
        return UtcTimeOnlyConverter.convert(localTime, calculatePrecision(localTime.getNano()));
    }

    @NotNull
    private String toDateOnly(FieldType fieldType, String value) {
        LocalDate localDate;
        try {
            localDate = LocalDate.parse(value);
        } catch (DateTimeParseException ex) {
            try {
                localDate = UtcDateOnlyConverter.convertToLocalDate(value);
            } catch (FieldConvertError error) {
                throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
            }
        }
        return UtcDateOnlyConverter.convert(localDate);
    }

    private UtcTimestampPrecision calculatePrecision(int nanos) {
        if (nanos == 0) {
            return UtcTimestampPrecision.SECONDS;
        }
        if (nanos % 1_000 != 0) {
            return UtcTimestampPrecision.NANOS;
        }
        if (nanos % 1_000_000 != 0) {
            return UtcTimestampPrecision.MICROS;
        }
        if (nanos % 1_000_000_000 != 0) {
            return UtcTimestampPrecision.MILLIS;
        }
        throw new IllegalArgumentException("nanos have incorrect value: " + nanos);
    }

    @Override
    public @NotNull MessageGroup decode(@NotNull MessageGroup messageGroup, @NotNull IReportingContext
            iReportingContext) {

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


        PushbackIterator<Field<?>> pushbackHeaderIterator = PushbackIterator.pushbackIterator(qfjMessage.getHeader().iterator());
        Message header = getMessage(pushbackHeaderIterator, transportDataDictionary, transportDataDictionary, qfjMessage.getHeader(), DataDictionary.HEADER_ID, Message.newBuilder(), null, null, null);
        builder.putFields(HEADER, ValueUtils.toValue(header));


        PushbackIterator<Field<?>> pushbackIterator = PushbackIterator.pushbackIterator(qfjMessage.iterator());
        fillMessageBody(pushbackIterator, builder, qfjMessage, msgType);


        PushbackIterator<Field<?>> pushbackTrailerIterator = PushbackIterator.pushbackIterator(qfjMessage.getTrailer().iterator());
        Message trailer = getMessage(pushbackTrailerIterator, transportDataDictionary, transportDataDictionary, qfjMessage.getTrailer(), DataDictionary.TRAILER_ID, Message.newBuilder(), null, null, null);
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
        String strMessage = new String(rawMessage, StandardCharsets.UTF_8);

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

    private void fillMessageBody(PushbackIterator<Field<?>> iterator, Message.Builder builder, quickfix.Message
            qfjMessage, String msgType) {

        DataDictionary dictionary = qfjMessage.isAdmin() ? transportDataDictionary : appDataDictionary;
        iterator.forEachRemaining(field -> {
            if (field == null) {
                LOGGER.warn("Null filed in the message with type {}, qfj msg {}", msgType, qfjMessage);
                return;
            }
            checkField(dictionary, iterator, field, qfjMessage, builder, msgType);
        });
    }

    private void checkField(DataDictionary dictionary, PushbackIterator<Field<?>> iterator, Field<?> field, FieldMap qfjMessage,
                            Message.Builder builder, String msgType) {
        if (useComponents && dictionary.isMsgComponentField(msgType, field.getTag())) {
            putComponent(iterator, field, dictionary, dictionary, qfjMessage, msgType, builder);
        } else {
            processField(field, dictionary, qfjMessage, builder, msgType, null);
        }
    }

    private void putComponent(PushbackIterator<Field<?>> iterator, Field<?> field, DataDictionary outerDictionary, DataDictionary dictionary,
                              FieldMap qfjMessage, String msgType, Message.Builder builder) {
        String componentName = dictionary.getMsgComponentName(msgType, field.getTag());
        Message innerMessage = getMessage(iterator, outerDictionary, dictionary, qfjMessage, msgType, Message.newBuilder(), field, null, componentName);
        builder.putFields(componentName, ValueUtils.toValue(innerMessage));
    }

    private void putField(DataDictionary dictionary, Message.Builder builder, int tag, String value) {

        String valueName = dictionary.getValueName(tag, value);
        builder.putFields(dictionary.getFieldName(tag), ValueUtils.toValue(valueName == null ? value : valueName));
    }

    private void fillListValue(ListValue.Builder listValue, DataDictionary outerDictionary, DataDictionary
            dataDictionary, List<Group> groups, Integer numInGroup, String msgType, String componentName) {

        for (Group group : groups) {
            Iterator<Field<?>> innerIterator = group.iterator();
            PushbackIterator<Field<?>> pushbackIterator = PushbackIterator.pushbackIterator(innerIterator);
            Message innerMessage = getMessage(pushbackIterator, outerDictionary, dataDictionary, group, msgType, Message.newBuilder(), null, numInGroup, componentName);
            listValue.addValues(ValueUtils.toValue(innerMessage));
        }
    }

    private boolean checkComponentGroups(String componentsName, DataDictionary outerDictionary, DataDictionary dataDictionary, int tag, Integer numInGroup, String msgType) {
        if (numInGroup != null) {
            return outerDictionary.isComponentField(componentsName, numInGroup) && dataDictionary.isField(tag);
        } else if (dataDictionary.isHeaderField(tag) || dataDictionary.isTrailerField(tag)) {
            return outerDictionary.isMsgComponentField(msgType, tag);
        }
        return false;
    }

    private Message getMessage(PushbackIterator<Field<?>> iterator, DataDictionary outerDictionary, DataDictionary dataDictionary, FieldMap
            fieldMap, String msgType, Message.Builder messageBuilder, Field<?> firstField, Integer numInGroup, String componentName) {

        if (firstField != null) {
            processField(firstField, dataDictionary, fieldMap, messageBuilder, msgType, componentName);
        }
        while (iterator.hasNext()) {
            Field<?> field = iterator.next();
            String fieldComponent = dataDictionary.getMsgComponentName(msgType, field.getTag());

            if (useComponents && fieldComponent != null && !fieldComponent.equals(componentName)) {
                if (dataDictionary.isComponentField(componentName, field.getTag())
                        || (dataDictionary.isMsgComponentField(msgType, field.getTag())
                        && checkComponentGroups(componentName, outerDictionary, dataDictionary, field.getTag(), numInGroup, msgType))) {

                    putComponent(iterator, field, outerDictionary, dataDictionary, fieldMap, msgType, messageBuilder);
                } else {
                    iterator.pushback(field);
                    return messageBuilder.build();
                }
            } else {
                if (useComponents && componentName != null && (!dataDictionary.isComponentField(componentName, field.getTag())
                        && !checkComponentGroups(componentName, outerDictionary, dataDictionary, field.getTag(), numInGroup, msgType))) {

                    iterator.pushback(field);
                    return messageBuilder.build();
                }
                processField(field, dataDictionary, fieldMap, messageBuilder, msgType, componentName);
            }
        }
        return messageBuilder.build();
    }

    private void processField(Field<?> field, DataDictionary dataDictionary, FieldMap fieldMap,
                              Message.Builder messageBuilder, String msgType, String componentName) {

        DataDictionary localDictionary = transportDataDictionary.isHeaderField(field.getField()) ||
                transportDataDictionary.isTrailerField(field.getField()) ? transportDataDictionary : appDataDictionary;

        if (dataDictionary.isGroup(msgType, field.getTag())) {
            @NotNull ListValue.Builder listValue = ValueUtils.listValue();
            List<Group> groups = fieldMap.getGroups(field.getTag());

            DataDictionary.GroupInfo groupInfo = Objects.requireNonNull(dataDictionary.getGroup(msgType, field.getTag()),
                    () -> "No GroupInfo for this combination of tag:{}" + field.getTag() + " and msgType:{}" + msgType);

            DataDictionary innerDataDictionary = groupInfo.getDataDictionary();

            fillListValue(listValue, dataDictionary, innerDataDictionary, groups, field.getTag(), msgType, componentName);
            messageBuilder.putFields(localDictionary.getFieldName(field.getTag()), Value.newBuilder().setListValue(listValue).build());
        } else {
            if (!fieldMap.isSetField(field.getField())) {
                throw new IllegalArgumentException("Invalid tag \"" + dataDictionary.getFieldName(field.getField()) + "\" for message group " + fieldMap);
            }

            String value = decodeFromType(localDictionary.getFieldType(field.getTag()), (String) field.getObject());
            if (replaceValuesWithEnumNames) {
                putField(localDictionary, messageBuilder, field.getTag(), value);
            } else {
                messageBuilder.putFields(localDictionary.getFieldName(field.getTag()), ValueUtils.toValue(value));
            }
        }
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

    @NotNull
    @Override
    public MessageGroup decode(@NotNull MessageGroup messageGroup) {
        return decode(messageGroup, new ReportingContext());
    }

    @NotNull
    @Override
    public MessageGroup encode(@NotNull MessageGroup messageGroup) {
        return encode(messageGroup, new ReportingContext());
    }
}