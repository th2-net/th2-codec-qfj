import com.exactpro.th2.codec.pipelineCodec.QFJCodec;
import com.exactpro.th2.codec.pipelineCodec.QFJCodecSettings;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.Group;
import quickfix.InvalidMessage;
import quickfix.field.BeginString;
import quickfix.field.MsgType;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoSides;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.SenderCompID;
import quickfix.field.Side;
import quickfix.field.TargetCompID;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class QFJCodecTest {

    private static QFJCodec codec;
    private static Message message;
    private static RawMessage rawMessage;

    @BeforeAll
    private static void initRawMessage(){
        quickfix.Message fixMessage = new quickfix.Message();
        fixMessage.getHeader().setField(new BeginString("FIXT.1.1"));
        fixMessage.getHeader().setField(new SenderCompID("client"));
        fixMessage.getHeader().setField(new TargetCompID("server"));
        fixMessage.getHeader().setField(new MsgType("AE"));

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

        Group noSidesGr2 = new Group(new NoSides().getField(), new Side().getField());
        noSidesGr2.setField(new Side('2'));

        noSidesGr1.addGroup(noPartyIDsGr1);
        noSidesGr1.addGroup(noPartyIDsGr2);

        fixMessage.addGroup(noSidesGr1);
        fixMessage.addGroup(noSidesGr2);

        byte[] bytes = fixMessage.toString().getBytes();

        rawMessage = RawMessage.newBuilder().setBody(ByteString.copyFrom(bytes)).build();
    }

    @BeforeAll
    private static void initMessage() {

        message = Message.newBuilder()
                .putFields("BeginString", Value.newBuilder().setSimpleValue("FIXT.1.1").build())
                .putFields("SenderCompID", Value.newBuilder().setSimpleValue("client").build())
                .putFields("TargetCompID", Value.newBuilder().setSimpleValue("server").build())
                .putFields("MsgSeqNum", Value.newBuilder().setSimpleValue("987").build())
                .putFields("NoSides", Value.newBuilder()
                        .setListValue(ListValue.newBuilder()
//                                .addValues(Value.newBuilder()
//                                        .setMessageValue(Message.newBuilder()
//                                                .putFields("LegSecurityID", Value.newBuilder().setSimpleValue("9891").build())
//                                                .putFields("LegSecurityIDSource", Value.newBuilder().setSimpleValue("8").build())
//                                                .putFields("LegSide", Value.newBuilder().setSimpleValue("2").build())
//                                                .putFields("LegRatioQty", Value.newBuilder().setSimpleValue("1").build())
//                                                .build())
//                                        .build())
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
                                                                                .putFields("PartyID", Value.newBuilder().setSimpleValue("party3").build())
                                                                                .putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                                                                .putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                                                                .putFields("QtyType", Value.newBuilder()
                                                                                        .setListValue(ListValue.newBuilder()
                                                                                                .addValues(Value.newBuilder().setMessageValue(Message.newBuilder()
                                                                                                        .putFields("YieldType", Value.newBuilder().setSimpleValue("yieldTypeValue").build())
                                                                                                        .putFields("Yield", Value.newBuilder().setSimpleValue("777").build())
                                                                                                        .build())
                                                                                                        .build())
                                                                                                .build())
                                                                                        .build())
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
//                                .addValues(Value.newBuilder()
//                                        .setMessageValue(Message.newBuilder()
//                                                .putFields("LegSecurityID", Value.newBuilder().setSimpleValue("4444").build())
//                                                .putFields("LegSecurityIDSource", Value.newBuilder().setSimpleValue("8").build())
//                                                .putFields("LegSide", Value.newBuilder().setSimpleValue("4").build())
//                                                .putFields("LegRatioQty", Value.newBuilder().setSimpleValue("4").build())
//                                                .build())
//                                        .build())
//
//                                .addValues(Value.newBuilder()
//                                        .setMessageValue(Message.newBuilder()
//                                                .putFields("LegSecurityID", Value.newBuilder().setSimpleValue("5665").build())
//                                                .putFields("LegSecurityIDSource", Value.newBuilder().setSimpleValue("8").build())
//                                                .putFields("LegSide", Value.newBuilder().setSimpleValue("3").build())
//                                                .putFields("LegRatioQty", Value.newBuilder().setSimpleValue("3").build())
//                                                .build())
//                                        .build())
                                .build())
                        .build())
                .setParentEventId(EventID.newBuilder().setId("ID12345").build())
                .setMetadata(MessageMetadata.newBuilder()
                        .setId(MessageID.newBuilder()
                                .setConnectionId(ConnectionID.newBuilder()
                                        .setSessionAlias("sessionAlias")
                                        .build())
                                .setDirection(Direction.SECOND)
                                .setSequence(11111111)
                                .build())
                        .setMessageType("AE")
                        .setTimestamp(Timestamp.newBuilder()
                                .setSeconds(Instant.now().getEpochSecond())
                                .setNanos(Instant.now().getNano()).build())
                        .build())
                .build();
    }

    @BeforeAll
    private static void initQFJCodec() throws ConfigError {
        codec = new QFJCodec(new QFJCodecSettings(), new DataDictionary("src/main/resources/FIX44.xml"),
                new DataDictionary("src/main/resources/FIXT11.xml"), new DataDictionary("src/main/resources/FIX50SP2.xml"));


    }

    @Test
    public void encodeMessageTest() {//todo add messageGroup testing
//        when(qfjCodec.encodeMessage(testMessage)).then()
        RawMessage resultRawMessage = codec.encodeMessage(message);

        System.out.println(resultRawMessage);
        assertTrue(true);
    }

    @Test
    public void decodeMessageTest() throws InvalidMessage {
        codec.decodeMessage(rawMessage);
    }
}
