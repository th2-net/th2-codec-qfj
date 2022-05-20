package com.exactpro.th2.codec.qfj

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.set
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import quickfix.ConfigError
import quickfix.DataDictionary
import quickfix.FieldException
import quickfix.Group
import quickfix.field.ApplID
import quickfix.field.BeginString
import quickfix.field.HopCompID
import quickfix.field.MsgType
import quickfix.field.OnBehalfOfCompID
import quickfix.field.PartyID
import quickfix.field.PartyIDSource
import quickfix.field.PartyRole
import quickfix.field.SenderCompID
import quickfix.field.Side
import quickfix.field.SignatureLength
import quickfix.field.TargetCompID
import quickfix.field.TestReqID
import java.time.Instant
import java.util.TreeMap
import org.junit.jupiter.api.TestInstance
import quickfix.field.NoHops
import quickfix.field.NoPartyIDs
import quickfix.field.NoSides
import quickfix.field.Signature

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QFJCodecTest {

    private lateinit var codec: QFJCodec
    private lateinit var messageGroup: MessageGroup
    private lateinit var messageGroupNoHeader: MessageGroup
    private lateinit var rawMessageGroup: MessageGroup
    private lateinit var strFixMessage: String
    private var timestampSeconds = Instant.now().epochSecond
    private var timestampNano = Instant.now().nano
    private var checksumValue: String = ""

    private fun getBodyLength(message: String): String {
        return message.substringAfter("\u00019=")
            .substringBefore("\u0001")
    }

    private fun getChecksum(message: String?): String {
        return message!!.substring(message.lastIndexOf("\u000110=") + 4, message.lastIndexOf("\u0001"))
    }

    @BeforeAll
    private fun initMessages() {
        val fixMessage = quickfix.Message()
        fixMessage.header.apply {
            setField(BeginString("FIXT.1.1"))
            setField(SenderCompID("client"))
            setField(TargetCompID("server"))
            setField(MsgType("AE"))
            addGroup(Group(NoHops.FIELD, HopCompID.FIELD).apply {
                setField(HopCompID("1"))
            })
            addGroup(Group(NoHops.FIELD, HopCompID.FIELD).apply {
                setField(HopCompID("2"))
            })
        }

        fixMessage.apply {
            setField(ApplID("111"))
            addGroup(Group(NoSides.FIELD, Side.FIELD).apply {
                setField(Side('1'))
                addGroup(Group(NoPartyIDs.FIELD, PartyID.FIELD).apply {
                    setField(PartyID("party1"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(11))
                })
                addGroup(Group(NoPartyIDs.FIELD, PartyID.FIELD).apply {
                    setField(PartyID("party2"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(56))
                })
            })

            addGroup(Group(NoSides.FIELD, Side.FIELD).apply {
                setField(Side('2'))
                addGroup(Group(NoPartyIDs.FIELD, PartyID.FIELD).apply {
                    setField(PartyID("party3"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(11))
                })
                addGroup(Group(NoPartyIDs.FIELD, PartyID.FIELD).apply {
                    setField(PartyID("party4"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(56))
                })
            })
        }

        fixMessage.trailer.apply {
            setField(SignatureLength(9))
            setField(Signature("signature"))
        }

        strFixMessage = fixMessage.toString()
        val bodyLength: String = getBodyLength(strFixMessage)
        checksumValue = getChecksum(strFixMessage)

        val bytes = fixMessage.toString().toByteArray()
        rawMessageGroup = getRawMessageGroup(bytes)

        val fieldsMap: MutableMap<String, Value> = HashMap()
        fieldsMap[QFJCodec.HEADER] = message().apply {
            this["BeginString"] = "FIXT.1.1"
            this["SenderCompID"] = "client"
            this["TargetCompID"] = "server"
            this["BodyLength"] = bodyLength
            this["MsgType"] = "AE"
            this["NoHops"] = listOf(
                message().addField("HopCompID", 1),
                message().addField("HopCompID", 2)
            )
        }.toValue()
        fieldsMap["ApplID"] = "111".toValue()
        fieldsMap["NoSides"] = ListValue.newBuilder().apply {
            listOf(
                addValues(message().apply {
                    this["Side"] = "1"
                    this["NoPartyIDs"] = listOf(
                        message().apply {
                            this["PartyID"] = "party1"
                            this["PartyIDSource"] = "D"
                            this["PartyRole"] = "11"
                        },
                        message().apply {
                            this["PartyID"] = "party2"
                            this["PartyIDSource"] = "D"
                            this["PartyRole"] = "56"
                        }
                    )
                }.toValue()),
                addValues(message().apply {
                    this["Side"] = "2"
                    this["NoPartyIDs"] = listOf(
                        message().apply {
                            this["PartyID"] = "party3"
                            this["PartyIDSource"] = "D"
                            this["PartyRole"] = "11"
                        },
                        message().apply {
                            this["PartyID"] = "party4"
                            this["PartyIDSource"] = "D"
                            this["PartyRole"] = "56"
                        }
                    )
                }.toValue())
            )
        }.toValue()

        fieldsMap[QFJCodec.TRAILER] = message().apply {
            this["CheckSum"] = checksumValue
            this["SignatureLength"] = "9"
            this["Signature"] = "signature"
        }.toValue()


        //INITIATING MESSAGE
        messageGroup = getMessageGroup(fieldsMap, "TradeCaptureReport")

        //INITIATING MESSAGE WITHOUT HEADER
        val fieldsMapNoHeader: MutableMap<String, Value> = TreeMap()
        fieldsMapNoHeader["NoSides"] = ListValue.newBuilder().apply {
            addValues(Value.newBuilder().apply {
                messageValue = Message.newBuilder().apply {
                    this["Side"] = "1"
                    this["NoPartyIDs"] = listOf(
                        message().apply {
                            this["PartyID"] = "party1"
                            this["PartyIDSource"] = "D"
                            this["PartyRole"] = "11"
                        },
                        message().apply {
                            this["PartyID"] = "party2"
                            this["PartyIDSource"] = "D"
                            this["PartyRole"] = "56"
                        }
                    )
                }.build()
            }.build())
        }.toValue()

        messageGroupNoHeader = getMessageGroup(fieldsMapNoHeader, "TradeCaptureReport")
    }

    private fun getMessageGroup(fieldsMap: MutableMap<String, Value>, msgType: String): MessageGroup {
        return MessageGroup.newBuilder()
            .addMessages(AnyMessage.newBuilder().apply {
                message = Message.newBuilder().apply {
                    putAllFields(fieldsMap)
                    parentEventId = EventID.newBuilder().apply {
                        id = "ID12345"
                    }.build()
                    metadata = MessageMetadata.newBuilder().apply {
                        id = MessageID.newBuilder().apply {
                            connectionId = ConnectionID.newBuilder().apply {
                                sessionAlias = "sessionAlias"
                            }.build()
                            direction = Direction.SECOND
                            sequence = 11111111
                        }.build()
                        messageType = msgType
                        protocol = "FIX"
                        timestamp = Timestamp.newBuilder().apply {
                            seconds = timestampSeconds
                            nanos = timestampNano
                        }.build()
                    }.build()
                }.build()
            }.build())
            .build()
    }

    private fun getRawMessageGroup(msg: ByteArray): MessageGroup {
        return MessageGroup.newBuilder()
            .addMessages(AnyMessage.newBuilder().apply {
                rawMessage = RawMessage.newBuilder().apply {
                    body = ByteString.copyFrom(msg)
                    parentEventId = EventID.newBuilder().apply {
                        id = "ID12345"
                    }.build()
                    metadata = RawMessageMetadata.newBuilder().apply {
                        id = MessageID.newBuilder().apply {
                            connectionId = ConnectionID.newBuilder().apply {
                                sessionAlias = "sessionAlias"
                            }.build()
                            direction = Direction.SECOND
                            sequence = 11111111
                        }.build()
                        protocol = "FIX"
                        timestamp = Timestamp.newBuilder().apply {
                            seconds = timestampSeconds
                            nanos = timestampNano
                        }.build()
                    }.build()
                }.build()
            }.build())
            .build()
    }

    @BeforeAll
    private fun initQFJCodec() {
        val settings = QFJCodecSettings()
        settings.isCheckFieldsOutOfOrder = true
        settings.isFixt = true
        codec = QFJCodec(
            settings,
            null,
            DataDictionary("src/test/resources/FIXT11.xml"),
            DataDictionary("src/test/resources/FIX50SP2.xml")
        )
    }

    @Test
    fun encodeTest() {
        val expectedMessageGroup = getRawMessageGroup(strFixMessage.toByteArray())

        val messageGroupResult = codec.encode(messageGroup)
        Assertions.assertEquals(expectedMessageGroup, messageGroupResult)
    }

    @Test
    fun encodeMessageWithoutHeaderTest() {
        val message = quickfix.Message()

        message.header.apply {
            setField(BeginString("FIXT.1.1"))
            setField(MsgType("AE"))
        }

        message.apply {
            addGroup(Group(NoSides.FIELD, Side.FIELD).apply {
                setField(Side('1'))
                addGroup(Group(NoPartyIDs.FIELD, PartyID.FIELD).apply {
                    setField(PartyID("party1"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(11))
                })
                addGroup(Group(NoPartyIDs.FIELD, PartyID.FIELD).apply {
                    setField(PartyID("party2"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(56))
                })
            })
        }
        val expectedMessage = message.toString()
        val expectedMessageGroup = getRawMessageGroup(expectedMessage.toByteArray())

        val messageGroupResult = codec.encode(messageGroupNoHeader)
        Assertions.assertEquals(expectedMessageGroup, messageGroupResult)
    }

    @Test
    fun decodeTest() {
        val expectedMessageGroup = messageGroup
        val result = codec.decode(rawMessageGroup)
        Assertions.assertEquals(expectedMessageGroup, result)
    }

    @Test
    @Throws(ConfigError::class)
    fun validateFieldsOutOfOrderTest() {

        val fixMessage = quickfix.Message()
        fixMessage.header.apply {
            setField(BeginString("FIXT.1.1"))
            setField(SenderCompID("client"))
            setField(TargetCompID("server"))
            setField(MsgType("0"))
            setField(TestReqID("testReqID")) //body tag in the header
        }
        fixMessage.setField(OnBehalfOfCompID("onBehalfOfCompID")) //header tag in the body
        val strFixMessage = fixMessage.toString()
        val bytes = fixMessage.toString().toByteArray()

        val rawMessageGroup = getRawMessageGroup(bytes)

        val bodyLength = getBodyLength(strFixMessage)
        val checksumValue = getChecksum(strFixMessage)

        val thrown = Assertions.assertThrows(
            IllegalStateException::class.java
        ) { codec.decode(rawMessageGroup) }
        Assertions.assertTrue(
            thrown
                .cause //IllegalStateException: Cannot decode raw message
            !!.cause //IllegalStateException: Cannot decode parsed message
                    is FieldException
        ) //FieldException: Tag specified out of required order, field=115
        Assertions.assertEquals(thrown.cause!!.cause!!.message, "Tag specified out of required order, field=115")

        //Disabled validateFieldsOutOfOrder
        val expectedFieldsMap: MutableMap<String, Value> = TreeMap()
        expectedFieldsMap[QFJCodec.HEADER] = Value.newBuilder()
            .setMessageValue(
                Message.newBuilder().apply {
                    this["BeginString"] = "FIXT.1.1"
                    this["SenderCompID"] = "client"
                    this["TargetCompID"] = "server"
                    this["BodyLength"] = bodyLength
                    this["OnBehalfOfCompID"] = "onBehalfOfCompID"
                    this["MsgType"] = "0"
                    build()
                })
            .build()
        expectedFieldsMap["TestReqID"] = Value.newBuilder().setSimpleValue("testReqID").build()
        expectedFieldsMap[QFJCodec.TRAILER] = Value.newBuilder().apply {
            messageValue = Message.newBuilder().apply {
                this["CheckSum"] = checksumValue
            }.build()
        }.build()

        val expectedMessageGroup = getMessageGroup(expectedFieldsMap, "Heartbeat")

        val anotherCodec = QFJCodec(
            QFJCodecSettings().apply {
                isCheckFieldsOutOfOrder = false
                isFixt = true
            },
            null,
            DataDictionary("src/test/resources/FIXT11.xml"),
            DataDictionary("src/test/resources/FIX50SP2.xml")
        )
        val result = anotherCodec.decode(rawMessageGroup)
        Assertions.assertEquals(expectedMessageGroup, result)
    }
}