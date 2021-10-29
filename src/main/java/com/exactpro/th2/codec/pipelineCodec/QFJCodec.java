package com.exactpro.th2.codec.pipelineCodec;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.exactpro.th2.common.grpc.AnyMessage;
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
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.InvalidMessage;
import quickfix.MessageUtils;
import quickfix.field.BeginString;
import quickfix.field.MsgType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class QFJCodec implements IPipelineCodec {

    private static final Logger LOGGER = LoggerFactory.getLogger(QFJCodec.class);
    private static final String PROTOCOL = "FIX";

    private IPipelineCodecSettings settings;
    private DataDictionary dataDictionary;
    private DataDictionary transportDataDictionary;
    private DataDictionary appDataDictionary;
    private DataDictionary localDataDictionary;

    public QFJCodec() {
    }

    public QFJCodec(IPipelineCodecSettings settings, DataDictionary dataDictionary) {
        this.settings = settings;
        this.dataDictionary = dataDictionary;
        this.transportDataDictionary = null;
        this.appDataDictionary = null;
    }

    public QFJCodec(IPipelineCodecSettings settings, DataDictionary dataDictionary,
                    DataDictionary transportDataDictionary, DataDictionary appDataDictionary) {
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

        String msgType = message.getMetadata().getMessageType();
        quickfix.Message fixMessage = getFixMessage(message.getFieldsMap(), msgType);

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

    private <T> quickfix.Message getFixMessage(Map<String, Value> fieldsMap, String msgType) {

        boolean isFix50 = isFix50(fieldsMap);
        if (isFix50) {
            localDataDictionary = appDataDictionary;
        } else {
            localDataDictionary = dataDictionary;
        }

        quickfix.Message message = new quickfix.Message();
        quickfix.Message.Header header = message.getHeader();
        quickfix.Message.Trailer trailer = message.getTrailer();

        String key;
        int tag;
        Value value;

        for (Map.Entry<String, Value> item : fieldsMap.entrySet()) {

            key = item.getKey();
            tag = validateTag(localDataDictionary.getFieldTag(key), key);
            value = item.getValue();

            LOGGER.trace("key: {}", key);
            LOGGER.trace("value: {}", value.getSimpleValue());
            LOGGER.trace("TAG №: " + tag);

            if (isGroup(value)) {
                for (Group group : getGroups(value, tag, localDataDictionary)) {
                    message.addGroup(group);
                }
            } else {
                Field<?> field = new Field<>(tag, value.getSimpleValue());

                if (isHeader(tag, isFix50)) {
                    header.setField(tag, field);//todo add header groups

                } else if (isTrailer(tag, isFix50)) {
                    trailer.setField(tag, field);

                } else {
                    if (!localDataDictionary.isMsgField(msgType, tag)) {
                        throw new IllegalStateException("tag \"" + tag + "\" does not belong to this type of message: " + msgType);
                    }
                    message.setField(tag, field);
                }
            }
        }
        message.getHeader().setField(new MsgType(msgType));

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

                LOGGER.trace("innerKey: {}", innerKey);
                LOGGER.trace("innerValue: {}", fieldsMapValue.getSimpleValue());
                LOGGER.trace("TAG №: {}", innerTag);

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

    private boolean isGroup(Value value) {
        return value.getListValue().getValuesCount() > 0;
    }

    private boolean isFix50(Map<String, Value> fieldsMap) {
        Value beginStringFix50 = Value.newBuilder().setSimpleValue("FIXT.1.1").build();
        return fieldsMap.containsValue(beginStringFix50);
    }

    private boolean isHeader(int tag, boolean isFix50) {
        if (isFix50) {
            return transportDataDictionary.isHeaderField(tag);
        } else {
            return dataDictionary.isHeaderField(tag);
        }
    }

    private boolean isTrailer(int tag, boolean isFix50) {
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
                                    throw new IllegalStateException("Cannot decode message " + anyMsg.getRawMessage()); //todo import toJson()
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
        boolean isFix50 = MessageUtils.getStringField(strMessage, BeginString.FIELD).equals("FIXT.1.1");

        if (isFix50) {
            qfjMessage.fromString(strMessage, transportDataDictionary, appDataDictionary, true, true);
            localDataDictionary = appDataDictionary;
        } else {
            qfjMessage.fromString(strMessage, dataDictionary, true, true);
            localDataDictionary = dataDictionary;
        }

        Map<String, Value> fieldsMap = new TreeMap<>(); //todo add comparator

        String msgType = "";
        try {
            msgType = qfjMessage.getHeader().getString(MsgType.FIELD);
        } catch (FieldNotFound fieldNotFound) {
            LOGGER.error("Cannot find message type in message: {}", qfjMessage);
        }

        Iterator<Field<?>> headerIterator = qfjMessage.getHeader().iterator();
        fillMessage(headerIterator, fieldsMap, qfjMessage, msgType, true);

        Iterator<Field<?>> iterator = qfjMessage.iterator();
        fillMessage(iterator, fieldsMap, qfjMessage, msgType, false);

        for (Map.Entry<String, Value> map: fieldsMap.entrySet()){
            System.out.println(map.getKey() + " " + map.getValue());
        }

        return Message.newBuilder()
                .setParentEventId(rawMessage.getParentEventId())
                .setMetadata(MessageMetadata.newBuilder()
                        .setId(rawMessage.getMetadata().getId())
                        .setTimestamp(rawMessage.getMetadata().getTimestamp())
                        .setProtocol(rawMessage.getMetadata().getProtocol())
                        .putAllProperties(rawMessage.getMetadata().getPropertiesMap())
                        .build())
        .build();
    }

    private void fillMessage(Iterator<Field<?>> iterator, Map<String, Value> fieldsMap, quickfix.Message qfjMessage,String msgType, boolean isFix50Header){

        while (iterator.hasNext()) {
            Field<?> field = iterator.next();

            if (isFix50Header){
                putField(transportDataDictionary, fieldsMap, field);
            }else {
                putField(localDataDictionary, fieldsMap, field);
            }

            if (localDataDictionary.isGroup(msgType, field.getTag())){

              //in progress...

            }
        }
    }

    private void putField(DataDictionary dataDictionary, Map<String, Value> fieldsMap, Field<?> field){
        fieldsMap.put(dataDictionary.getFieldName(field.getTag()), ValueUtils.toValue(field.getObject()));
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
}