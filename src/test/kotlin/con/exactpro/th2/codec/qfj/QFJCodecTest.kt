package con.exactpro.th2.codec.qfj

import com.exactpro.th2.codec.qfj.QFJCodec
import com.exactpro.th2.codec.qfj.QFJCodecSettings
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
import quickfix.field.NoHops
import quickfix.field.NoPartyIDs
import quickfix.field.NoSides
import quickfix.field.Signature

class QFJCodecTest {

    companion object {
        private var codec: QFJCodec? = null
        private var messageGroup: MessageGroup? = null
        private var messageGroupNoHeader: MessageGroup? = null
        private var rawMessageGroup: MessageGroup? = null
        private var strFixMessage: String? = null
        private var timestampSeconds = Instant.now().epochSecond
        private var timestampNano = Instant.now().nano
        private var checksumValue: String = ""

        private fun getBodyLength(message: String?): String {
            return message!!.substring(
                message.indexOf("\u00019=") + 3,
                message.indexOf("\u0001", message.indexOf("\u00019=") + 1)
            )
        }

        fun getChecksum(message: String?): String {
            return message!!.substring(message.lastIndexOf("\u000110=") + 4, message.lastIndexOf("\u0001"))
        }

        @BeforeAll
        @JvmStatic
        private fun initMessages() {
            val fixMessage = quickfix.Message()
            fixMessage.header.apply {
                setField(BeginString("FIXT.1.1"))
                setField(SenderCompID("client"))
                setField(TargetCompID("server"))
                setField(MsgType("AE"))
                addGroup(Group(NoHops().field, HopCompID().field).apply {
                    setField(HopCompID("1"))
                })
                addGroup(Group(NoHops().field, HopCompID().field).apply {
                    setField(HopCompID("2"))
                })
            }

            fixMessage.apply {
                setField(ApplID("111"))
                addGroup(Group(NoSides().field, Side().field).apply {
                    setField(Side('1'))
                    addGroup(Group(NoPartyIDs().field, PartyID().field).apply {
                        setField(PartyID("party1"))
                        setField(PartyIDSource('D'))
                        setField(PartyRole(11))
                    })
                    addGroup(Group(NoPartyIDs().field, PartyID().field).apply {
                        setField(PartyID("party2"))
                        setField(PartyIDSource('D'))
                        setField(PartyRole(56))
                    })
                })

                addGroup(Group(NoSides().field, Side().field).apply {
                    setField(Side('2'))
                    addGroup(Group(NoPartyIDs().field, PartyID().field).apply {
                        setField(PartyID("party3"))
                        setField(PartyIDSource('D'))
                        setField(PartyRole(11))
                    })
                    addGroup(Group(NoPartyIDs().field, PartyID().field).apply {
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

            val fieldsMap: MutableMap<String, Value> = TreeMap()
            fieldsMap[QFJCodec.HEADER] = Value.newBuilder().apply {
                messageValue = Message.newBuilder().apply {
                    putFields("BeginString", Value.newBuilder().setSimpleValue("FIXT.1.1").build())
                    putFields("SenderCompID", Value.newBuilder().setSimpleValue("client").build())
                    putFields("TargetCompID", Value.newBuilder().setSimpleValue("server").build())
                    putFields("BodyLength", Value.newBuilder().setSimpleValue(bodyLength).build())
                    putFields("MsgType", Value.newBuilder().setSimpleValue("AE").build())
                    putFields("NoHops", Value.newBuilder().apply {
                        listValue = ListValue.newBuilder().apply {
                            addValues(Value.newBuilder().apply {
                                messageValue = Message.newBuilder().apply {
                                    putFields("HopCompID", Value.newBuilder().setSimpleValue("1").build())
                                }.build()
                            }.build())
                            addValues(Value.newBuilder().apply {
                                messageValue = Message.newBuilder().apply {
                                    putFields("HopCompID", Value.newBuilder().setSimpleValue("2").build())
                                }.build()
                            }.build())
                        }.build()
                    }.build())
                }.build()
            }.build()
            fieldsMap["ApplID"] = Value.newBuilder().setSimpleValue("111").build()
            fieldsMap["NoSides"] = Value.newBuilder().apply {
                listValue = ListValue.newBuilder().apply {
                    addValues(Value.newBuilder().apply {
                        messageValue = Message.newBuilder().apply {
                            putFields("Side", Value.newBuilder().setSimpleValue("1").build())
                            putFields("NoPartyIDs", Value.newBuilder().apply {
                                listValue = ListValue.newBuilder().apply {
                                    addValues(Value.newBuilder().apply {
                                        messageValue = Message.newBuilder().apply {
                                            putFields("PartyID", Value.newBuilder().setSimpleValue("party1").build())
                                            putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                            putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                        }.build()
                                    }.build())
                                    addValues(Value.newBuilder().apply {
                                        messageValue = Message.newBuilder().apply {
                                            putFields("PartyID", Value.newBuilder().setSimpleValue("party2").build())
                                            putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                            putFields("PartyRole", Value.newBuilder().setSimpleValue("56").build())
                                        }.build()
                                    }.build())
                                }.build()
                            }.build())
                        }.build()
                    }.build())
                    addValues(Value.newBuilder().apply {
                        messageValue = Message.newBuilder().apply {
                            putFields("Side", Value.newBuilder().setSimpleValue("2").build())
                            putFields("NoPartyIDs", Value.newBuilder().apply {
                                listValue = ListValue.newBuilder().apply {
                                    addValues(Value.newBuilder().apply {
                                        messageValue = Message.newBuilder().apply {
                                            putFields("PartyID", Value.newBuilder().setSimpleValue("party3").build())
                                            putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                            putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                        }.build()
                                    }.build())
                                    addValues(Value.newBuilder().apply {
                                        messageValue = Message.newBuilder().apply {
                                            putFields("PartyID", Value.newBuilder().setSimpleValue("party4").build())
                                            putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                            putFields("PartyRole", Value.newBuilder().setSimpleValue("56").build())
                                        }.build()
                                    }.build())
                                }.build()
                            }.build())
                        }.build()
                    }.build())
                }.build()
            }.build()
            fieldsMap[QFJCodec.TRAILER] = Value.newBuilder().apply {
                messageValue = Message.newBuilder()
                    .putFields("CheckSum", Value.newBuilder().setSimpleValue(checksumValue).build())
                    .putFields("SignatureLength", Value.newBuilder().setSimpleValue("9").build())
                    .putFields("Signature", Value.newBuilder().setSimpleValue("signature").build())
                    .build()
            }.build()

            //INITIATING MESSAGE
            messageGroup = getMessageGroup(fieldsMap, "TradeCaptureReport")

            //INITIATING MESSAGE WITHOUT HEADER
            val fieldsMapNoHeader: MutableMap<String, Value> = TreeMap()
            fieldsMapNoHeader["NoSides"] = Value.newBuilder().apply {
                listValue = ListValue.newBuilder().apply {
                    addValues(Value.newBuilder().apply {
                        messageValue = Message.newBuilder().apply {
                            putFields("Side", Value.newBuilder().setSimpleValue("1").build())
                            putFields("NoPartyIDs", Value.newBuilder().apply {
                                listValue = ListValue.newBuilder().apply {
                                    addValues(Value.newBuilder().apply {
                                        messageValue = Message.newBuilder().apply {
                                            putFields("PartyID", Value.newBuilder().setSimpleValue("party1").build())
                                            putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                            putFields("PartyRole", Value.newBuilder().setSimpleValue("11").build())
                                        }.build()
                                    }.build())
                                    addValues(Value.newBuilder().apply {
                                        messageValue = Message.newBuilder().apply {
                                            putFields("PartyID", Value.newBuilder().setSimpleValue("party2").build())
                                            putFields("PartyIDSource", Value.newBuilder().setSimpleValue("D").build())
                                            putFields("PartyRole", Value.newBuilder().setSimpleValue("56").build())
                                        }.build()
                                    }.build())
                                }.build()
                            }.build())
                        }.build()
                    }.build())
                }.build()
            }.build()

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
        @JvmStatic
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
    }

    @Test
    fun encodeTest() {
        val expectedMessageGroup = getRawMessageGroup(strFixMessage!!.toByteArray())

        val messageGroupResult = codec!!.encode(messageGroup!!)
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
            addGroup(Group(NoSides().field, Side().field).apply {
                setField(Side('1'))
                addGroup(Group(NoPartyIDs().field, PartyID().field).apply {
                    setField(PartyID("party1"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(11))
                })
                addGroup(Group(NoPartyIDs().field, PartyID().field).apply {
                    setField(PartyID("party2"))
                    setField(PartyIDSource('D'))
                    setField(PartyRole(56))
                })
            })
        }
        val expectedMessage = message.toString()
        val expectedMessageGroup = getRawMessageGroup(expectedMessage.toByteArray())

        val messageGroupResult = codec!!.encode(messageGroupNoHeader!!)
        Assertions.assertEquals(expectedMessageGroup, messageGroupResult)
    }

    @Test
    fun decodeTest() {
        val expectedMessageGroup = messageGroup
        val result = codec!!.decode(rawMessageGroup!!)
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
        ) { codec!!.decode(rawMessageGroup) }
        Assertions.assertTrue(
            thrown
            !!.cause //IllegalStateException: Cannot decode raw message
            !!.cause //IllegalStateException: Cannot decode parsed message
                    is FieldException
        ) //FieldException: Tag specified out of required order, field=115
        Assertions.assertEquals(thrown.cause!!.cause!!.message, "Tag specified out of required order, field=115")

        //Disabled validateFieldsOutOfOrder
        val expectedFieldsMap: MutableMap<String, Value> = TreeMap()
        expectedFieldsMap[QFJCodec.HEADER] = Value.newBuilder()
            .setMessageValue(
                Message.newBuilder().apply {
                    putFields("BeginString", Value.newBuilder().setSimpleValue("FIXT.1.1").build())
                    putFields("SenderCompID", Value.newBuilder().setSimpleValue("client").build())
                    putFields("TargetCompID", Value.newBuilder().setSimpleValue("server").build())
                    putFields("BodyLength", Value.newBuilder().setSimpleValue(bodyLength).build())
                    putFields("OnBehalfOfCompID", Value.newBuilder().setSimpleValue("onBehalfOfCompID").build())
                    putFields("MsgType", Value.newBuilder().setSimpleValue("0").build())
                    build()
                })
            .build()
        expectedFieldsMap["TestReqID"] = Value.newBuilder().setSimpleValue("testReqID").build()
        expectedFieldsMap[QFJCodec.TRAILER] = Value.newBuilder().apply {
            messageValue = Message.newBuilder().apply {
                putFields("CheckSum", Value.newBuilder().setSimpleValue(checksumValue).build())
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