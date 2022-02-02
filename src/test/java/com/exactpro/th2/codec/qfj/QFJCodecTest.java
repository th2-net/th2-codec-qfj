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

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.Group;
import quickfix.field.ApplID;
import quickfix.field.BeginString;
import quickfix.field.HopCompID;
import quickfix.field.MsgType;
import quickfix.field.NoHops;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoSides;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.SenderCompID;
import quickfix.field.Side;
import quickfix.field.Signature;
import quickfix.field.SignatureLength;
import quickfix.field.TargetCompID;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class QFJCodecTest {

    private static QFJCodec codec;
    private static MessageGroup messageGroup;
    private static MessageGroup messageGroupNoHeader;
    private static long timestampSeconds;
    private static int timestampNano;
    private static MessageGroup rawMessageGroup;
    private static String strFixMessage;


    @BeforeAll
    private static void initMessages() {
        timestampSeconds = Instant.now().getEpochSecond();
        timestampNano = Instant.now().getNano();
        String checksumValue;
        String bodyLength;

        //INITIATING RAW MESSAGE
        quickfix.Message fixMessage = new quickfix.Message();
        fixMessage.getHeader().setField(new BeginString("FIXT.1.1"));
        fixMessage.getHeader().setField(new SenderCompID("client"));
        fixMessage.getHeader().setField(new TargetCompID("server"));
        fixMessage.getHeader().setField(new MsgType("AE"));

        //ADDING HEADER GROUPS
        Group headerGroup = new Group(new NoHops().getField(), new HopCompID().getField());
        headerGroup.setField(new HopCompID("1"));

        Group headerGroup2 = new Group(new NoHops().getField(), new HopCompID().getField());
        headerGroup2.setField(new HopCompID("2"));

        fixMessage.getHeader().addGroup(headerGroup);
        fixMessage.getHeader().addGroup(headerGroup2);


        Group noSidesGr1 = new Group(new NoSides().getField(), new Side().getField());
        noSidesGr1.setField(new Side('1'));

        Group noSidesGr2 = new Group(new NoSides().getField(), new Side().getField());
        noSidesGr2.setField(new Side('2'));

        Group noPartyIDsGr1 = new Group(new NoPartyIDs().getField(), new PartyID().getField());
        noPartyIDsGr1.setField(new PartyID("party1"));
        noPartyIDsGr1.setField(new PartyIDSource('D'));
        noPartyIDsGr1.setField(new PartyRole(11));

        Group noPartyIDsGr2 = new Group(new NoPartyIDs().getField(), new PartyID().getField());
        noPartyIDsGr2.setField(new PartyID("party2"));
        noPartyIDsGr2.setField(new PartyIDSource('D'));
        noPartyIDsGr2.setField(new PartyRole(56));

        noSidesGr1.addGroup(noPartyIDsGr1);
        noSidesGr1.addGroup(noPartyIDsGr2);

        Group noPartyIDsGr3 = new Group(new NoPartyIDs().getField(), new PartyID().getField());
        noPartyIDsGr3.setField(new PartyID("party3"));
        noPartyIDsGr3.setField(new PartyIDSource('D'));
        noPartyIDsGr3.setField(new PartyRole(11));

        Group noPartyIDsGr4 = new Group(new NoPartyIDs().getField(), new PartyID().getField());
        noPartyIDsGr4.setField(new PartyID("party4"));
        noPartyIDsGr4.setField(new PartyIDSource('D'));
        noPartyIDsGr4.setField(new PartyRole(56));

        noSidesGr2.addGroup(noPartyIDsGr3);
        noSidesGr2.addGroup(noPartyIDsGr4);

        fixMessage.setField(new ApplID("111"));

        fixMessage.addGroup(noSidesGr1);
        fixMessage.addGroup(noSidesGr2);

        fixMessage.getTrailer().setField(new SignatureLength(9));
        fixMessage.getTrailer().setField(new Signature("signature"));

        strFixMessage = fixMessage.toString();
        bodyLength = strFixMessage.substring(strFixMessage.indexOf("\0019=") + 3, strFixMessage.indexOf("\001", strFixMessage.indexOf("\0019=") + 1));
        checksumValue = strFixMessage.substring(strFixMessage.lastIndexOf("\00110=") + 4, strFixMessage.lastIndexOf("\001"));

        byte[] bytes = fixMessage.toString().getBytes();

        rawMessageGroup = MessageGroup.newBuilder()
                .addMessages(AnyMessage.newBuilder()
                        .setRawMessage(RawMessage.newBuilder()
                                .setBody(ByteString.copyFrom(bytes))
                                .setMetadata(RawMessageMetadata.newBuilder()
                                        .setId(MessageID.newBuilder()
                                                .setConnectionId(ConnectionID.newBuilder()
                                                        .setSessionAlias("sessionAlias")
                                                        .build())
                                                .setDirection(Direction.SECOND)
                                                .setSequence(11111111)
                                                .build())
                                        .setProtocol("FIX")
                                        .setTimestamp(Timestamp.newBuilder()
                                                .setSeconds(timestampSeconds)
                                                .setNanos(timestampNano)
                                                .build())
                                        .build())
                                .setParentEventId(EventID.newBuilder().setId("ID12345").build())
                                .build())
                        .build())
                .build();

        //INITIATING MESSAGE
        Map<String, Value> fieldsMap = new TreeMap<>();
        fieldsMap.put("Header", Value.newBuilder()
                .setMessageValue(Message.newBuilder()
                        .putFields("BeginString", Value.newBuilder().setSimpleValue("FIXT.1.1").build())
                        .putFields("SenderCompID", Value.newBuilder().setSimpleValue("client").build())
                        .putFields("TargetCompID", Value.newBuilder().setSimpleValue("server").build())
                        .putFields("BodyLength", Value.newBuilder().setSimpleValue(bodyLength).build())
                        .putFields("MsgType", Value.newBuilder().setSimpleValue("AE").build())
                        .putFields("NoHops", Value.newBuilder()
                                .setListValue(ListValue.newBuilder()
                                        .addValues(Value.newBuilder()
                                                .setMessageValue(Message.newBuilder()
                                                        .putFields("HopCompID", Value.newBuilder().setSimpleValue("1").build())
                                                        .build())
                                                .build())
                                        .addValues(Value.newBuilder()
                                                .setMessageValue(Message.newBuilder()
                                                        .putFields("HopCompID", Value.newBuilder().setSimpleValue("2").build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
        fieldsMap.put("ApplID", Value.newBuilder().setSimpleValue("111").build());
        fieldsMap.put("NoSides", Value.newBuilder()
                .setListValue(ListValue.newBuilder()
                        .addValues(Value.newBuilder()
                                .setMessageValue(Message.newBuilder()
                                        .putFields("Side", Value.newBuilder().setSimpleValue("1").build())
                                        .putFields("NoPartyIDs", Value.newBuilder()
                                                .setListValue(ListValue.newBuilder()
                                                        .addValues(Value.newBuilder()
                                                                .setMessageValue(Message.newBuilder()
                                                                        .putFields("PartyID", Value.newBuilder().setSimpleValue("party1").build())
                                                                        .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                        .putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                                                        .build())
                                                                .build())
                                                        .addValues(Value.newBuilder()
                                                                .setMessageValue(Message.newBuilder()
                                                                        .putFields("PartyID", Value.newBuilder().setSimpleValue("party2").build())
                                                                        .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                        .putFields("PartyRole", Value.newBuilder().setSimpleValue("56").build())
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .addValues(Value.newBuilder()
                                .setMessageValue(Message.newBuilder()
                                        .putFields("Side", Value.newBuilder().setSimpleValue("2").build())
                                        .putFields("NoPartyIDs", Value.newBuilder()
                                                .setListValue(ListValue.newBuilder()
                                                        .addValues(Value.newBuilder()
                                                                .setMessageValue(Message.newBuilder()
                                                                        .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                        .putFields("PartyID", Value.newBuilder().setSimpleValue("party3").build())
                                                                        .putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                                                        .build())
                                                                .build())
                                                        .addValues(Value.newBuilder()
                                                                .setMessageValue(Message.newBuilder()
                                                                        .putFields("PartyID", Value.newBuilder().setSimpleValue("party4").build())
                                                                        .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                        .putFields("PartyRole", Value.newBuilder().setSimpleValue("56").build())
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
        fieldsMap.put("Trailer", Value.newBuilder()
                .setMessageValue(Message.newBuilder()
                        .putFields("CheckSum", Value.newBuilder().setSimpleValue(checksumValue).build())
                        .putFields("SignatureLength", Value.newBuilder().setSimpleValue("9").build())
                        .putFields("Signature", Value.newBuilder().setSimpleValue("signature").build())
                        .build())
                .build());

        messageGroup = MessageGroup.newBuilder()
                .addMessages(AnyMessage.newBuilder()
                        .setMessage(Message.newBuilder()
                                .putAllFields(fieldsMap)
                                .setParentEventId(EventID.newBuilder().setId("ID12345").build())
                                .setMetadata(MessageMetadata.newBuilder()
                                        .setId(MessageID.newBuilder()
                                                .setConnectionId(ConnectionID.newBuilder()
                                                        .setSessionAlias("sessionAlias")
                                                        .build())
                                                .setDirection(Direction.SECOND)
                                                .setSequence(11111111)
                                                .build())
                                        .setMessageType("TradeCaptureReport")
                                        .setProtocol("FIX")
                                        .setTimestamp(Timestamp.newBuilder()
                                                .setSeconds(timestampSeconds)
                                                .setNanos(timestampNano)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        //INITIATING MESSAGE WITHOUT HEADER
        Map<String, Value> fieldsMapNoHeader = new TreeMap<>();
        fieldsMapNoHeader.put("NoSides", Value.newBuilder()
                .setListValue(ListValue.newBuilder()
                        .addValues(Value.newBuilder()
                                .setMessageValue(Message.newBuilder()
                                        .putFields("Side", Value.newBuilder().setSimpleValue("1").build())
                                        .putFields("NoPartyIDs", Value.newBuilder()
                                                .setListValue(ListValue.newBuilder()
                                                        .addValues(Value.newBuilder()
                                                                .setMessageValue(Message.newBuilder()
                                                                        .putFields("PartyID", Value.newBuilder().setSimpleValue("party1").build())
                                                                        .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                        .putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                                                        .build())
                                                                .build())
                                                        .addValues(Value.newBuilder()
                                                                .setMessageValue(Message.newBuilder()
                                                                        .putFields("PartyID", Value.newBuilder().setSimpleValue("party2").build())
                                                                        .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                        .putFields("PartyRole", Value.newBuilder().setSimpleValue("56").build())
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

        messageGroupNoHeader = MessageGroup.newBuilder()
                .addMessages(AnyMessage.newBuilder()
                        .setMessage(Message.newBuilder()
                                .putAllFields(fieldsMapNoHeader)
                                .setParentEventId(EventID.newBuilder().setId("ID12345").build())
                                .setMetadata(MessageMetadata.newBuilder()
                                        .setId(MessageID.newBuilder()
                                                .setConnectionId(ConnectionID.newBuilder()
                                                        .setSessionAlias("sessionAlias")
                                                        .build())
                                                .setDirection(Direction.SECOND)
                                                .setSequence(11111111)
                                                .build())
                                        .setMessageType("TradeCaptureReport")
                                        .setProtocol("FIX")
                                        .setTimestamp(Timestamp.newBuilder()
                                                .setSeconds(timestampSeconds)
                                                .setNanos(timestampNano)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @BeforeAll
    private static void initQFJCodec() throws ConfigError {
        codec = new QFJCodec(null, new DataDictionary("src/test/resources/FIXT11.xml"), new DataDictionary("src/test/resources/FIX50SP2.xml"));
//        codec = new QFJCodec(new DataDictionary("src/test/resources/FIX44.xml"), null, null);

    }

    @Test
    public void encodeTest() {

        MessageGroup expectedMessageGroup = MessageGroup.newBuilder()
                .addMessages(AnyMessage.newBuilder()
                        .setRawMessage(RawMessage.newBuilder()
                                .setBody(ByteString.copyFrom(strFixMessage.getBytes()))
                                .setMetadata(RawMessageMetadata.newBuilder()
                                        .setId(MessageID.newBuilder()
                                                .setConnectionId(ConnectionID.newBuilder()
                                                        .setSessionAlias("sessionAlias")
                                                        .build())
                                                .setDirection(Direction.SECOND)
                                                .setSequence(11111111)
                                                .build())
                                        .setProtocol("FIX")
                                        .setTimestamp(Timestamp.newBuilder()
                                                .setSeconds(timestampSeconds)
                                                .setNanos(timestampNano)
                                                .build())
                                        .build())
                                .setParentEventId(EventID.newBuilder().setId("ID12345").build())
                                .build())
                        .build())
                .build();

        MessageGroup messageGroupResult = codec.encode(messageGroup);
        assertEquals(messageGroupResult, expectedMessageGroup);
    }

    @Test
    public void encodeMessageWithoutHeaderTest() {

        quickfix.Message message = new quickfix.Message();
        message.getHeader().setField(new BeginString("FIXT.1.1"));
        message.getHeader().setField(new MsgType("AE"));

        Group noSidesGr1 = new Group(new NoSides().getField(), new Side().getField());
        noSidesGr1.setField(new Side('1'));

        Group noPartyIDsGr1 = new Group(new NoPartyIDs().getField(), new PartyID().getField());
        noPartyIDsGr1.setField(new PartyID("party1"));
        noPartyIDsGr1.setField(new PartyIDSource('D'));
        noPartyIDsGr1.setField(new PartyRole(11));

        Group noPartyIDsGr2 = new Group(new NoPartyIDs().getField(), new PartyID().getField());
        noPartyIDsGr2.setField(new PartyID("party2"));
        noPartyIDsGr2.setField(new PartyIDSource('D'));
        noPartyIDsGr2.setField(new PartyRole(56));

        noSidesGr1.addGroup(noPartyIDsGr1);
        noSidesGr1.addGroup(noPartyIDsGr2);

        message.addGroup(noSidesGr1);
        String expectedMessage = message.toString();

        MessageGroup expectedMessageGroup = MessageGroup.newBuilder()
                .addMessages(AnyMessage.newBuilder()
                        .setRawMessage(RawMessage.newBuilder()
                                .setBody(ByteString.copyFrom(expectedMessage.getBytes()))
                                .setMetadata(RawMessageMetadata.newBuilder()
                                        .setId(MessageID.newBuilder()
                                                .setConnectionId(ConnectionID.newBuilder()
                                                        .setSessionAlias("sessionAlias")
                                                        .build())
                                                .setDirection(Direction.SECOND)
                                                .setSequence(11111111)
                                                .build())
                                        .setProtocol("FIX")
                                        .setTimestamp(Timestamp.newBuilder()
                                                .setSeconds(timestampSeconds)
                                                .setNanos(timestampNano)
                                                .build())
                                        .build())
                                .setParentEventId(EventID.newBuilder().setId("ID12345").build())
                                .build())
                        .build())
                .build();

        MessageGroup messageGroupResult = codec.encode(messageGroupNoHeader);
        assertEquals(messageGroupResult, expectedMessageGroup);
    }

    @Test
    public void decodeTest() {
        MessageGroup expectedMessageGroup = messageGroup;

        MessageGroup result = codec.decode(rawMessageGroup);
        assertEquals(result, expectedMessageGroup);
    }
}
