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
package com.exactpro.th2.codec.qfj

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.set
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import java.time.Instant
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import quickfix.ConfigError
import quickfix.DataDictionary
import quickfix.FieldException
import quickfix.Group
import quickfix.field.ApplID
import quickfix.field.BeginString
import quickfix.field.HopCompID
import quickfix.field.MsgType
import quickfix.field.NoHops
import quickfix.field.NoPartyIDs
import quickfix.field.NoSides
import quickfix.field.OnBehalfOfCompID
import quickfix.field.PartyID
import quickfix.field.PartyIDSource
import quickfix.field.PartyRole
import quickfix.field.SenderCompID
import quickfix.field.Side
import quickfix.field.Signature
import quickfix.field.SignatureLength
import quickfix.field.TargetCompID
import quickfix.field.TestReqID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QFJCodecTest {

    private val codec: QFJCodec
    private val messageGroup: MessageGroup
    private val rawMessageGroup: MessageGroup
    private val messageGroupNoHeader: MessageGroup
    private val strFixMessage: String
    private val timestamp: Timestamp = Instant.now().toTimestamp()
    private var checksumValue: String = ""

    init {
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

        val msgBuilder = message().apply {
            this[QFJCodec.HEADER] = message().apply {
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

            this["ApplID"] = "111".toValue()
            this["NoSides"] = listOf(
                message().apply {
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
                    ).toValue()
                },
                message().apply {
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
                    ).toValue()
                }
            ).toValue()

            this[QFJCodec.TRAILER] = message().apply {
                this["CheckSum"] = checksumValue
                this["SignatureLength"] = "9"
                this["Signature"] = "signature"
            }.toValue()
        }

        //INITIATING MESSAGE
        messageGroup = getMessageGroup(msgBuilder, "TradeCaptureReport")

        //INITIATING MESSAGE WITHOUT HEADER
        val msgBuilderNoHeader = message().apply {
            this["NoSides"] = listOf(
                message().apply {
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
                }
            ).toValue()
        }
        messageGroupNoHeader = getMessageGroup(msgBuilderNoHeader, "TradeCaptureReport")
    }

    init {
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

    private fun getBodyLength(message: String): String {
        return message.substringAfter("\u00019=")
            .substringBefore('\u0001')
    }

    private fun getChecksum(message: String): String {
        return message.substringAfterLast("\u000110=").substringBefore('\u0001')
    }

    private fun getMessageGroup(msgBuilder: Message.Builder, msgType: String) =
        MessageGroup.newBuilder().apply {
            this += AnyMessage.newBuilder().messageBuilder.apply {
                parentEventIdBuilder.id = "ID12345"
                messageType = msgType
                sessionAlias = "sessionAlias"
                direction = Direction.SECOND
                sequence = 11111111
                metadataBuilder.apply {
                    protocol = "FIX"
                    timestamp = this@QFJCodecTest.timestamp
                }
                putAllFields(msgBuilder.fieldsMap)
            }
        }.build()

    private fun getRawMessageGroup(msg: ByteArray): MessageGroup =
        MessageGroup.newBuilder().apply {
            this += AnyMessage.newBuilder().rawMessageBuilder.apply {
                parentEventIdBuilder.id = "ID12345"
                sessionAlias = "sessionAlias"
                direction = Direction.SECOND
                sequence = 11111111
                metadataBuilder.apply {
                    timestamp = this@QFJCodecTest.timestamp
                    protocol = "FIX"
                }
                body = ByteString.copyFrom(msg)
            }
        }.build()

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

        val thrown = Assertions.assertThrows(IllegalStateException::class.java) {
            codec.decode(rawMessageGroup)
        }
        val fieldException = Assertions.assertInstanceOf(FieldException::class.java, thrown.cause?.cause)
        Assertions.assertEquals(fieldException.message, "Tag specified out of required order, field=115")

        //Disabled validateFieldsOutOfOrder
        val expectedMsgBuilder = message().apply {
            this[QFJCodec.HEADER] = message().apply {
                this["BeginString"] = "FIXT.1.1"
                this["SenderCompID"] = "client"
                this["TargetCompID"] = "server"
                this["BodyLength"] = bodyLength
                this["OnBehalfOfCompID"] = "onBehalfOfCompID"
                this["MsgType"] = "0"
            }.toValue()

            this["TestReqID"] = "testReqID".toValue()

            this[QFJCodec.TRAILER] = message()
                .addField("CheckSum", checksumValue).toValue()
        }

        val expectedMessageGroup = getMessageGroup(expectedMsgBuilder, "Heartbeat")

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