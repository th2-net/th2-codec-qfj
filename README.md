# th2-codec-qfj

## Configuration

TODO: add infromation about dictionaries and configuration 

MsgType of the message is taken from the enum of the names of msgType(35) field in the dictionary

### Configuration example

```yaml
fixt: true
checkFieldsOutOfOrder: true
replaceValuesWithEnumNames: false
```

* fixt - codec-qfj should be configured by two dictionaries: MAIN business and LEVEL1 session levels other ways only the MAIN dictionary with messages from both layers is required.
* checkFieldsOutOfOrder - controls whether out of order fields are checked. Default value is true.
  This option is useful when messages to decode have a fields order that differs from the order defined in the configured dictionary. 
  Example of the particular case: header's fields are mixed with body's fields.  
* replaceValuesWithEnumNames - (default false) replace values with its enum names if there are enum values for the tag.  
  Example:
  The value `B` for tag `4` should be replaced with `BUY` value. If enum value is missing the original value should be left.
  If a field does not contain values the value should be left unchanged.
```xml
<field number="4" name="AdvSide" type="STRING">
  <value enum="B" description="BUY"/>
</field>
```

For example:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec-qfj
spec:
  custom-config:
    codecSettings:
      fixt: true
      checkFieldsOutOfOrder: true
      replaceValuesWithEnumNames: false
```

## Required pins

Every type of connection has two `subscribe` and `publish` pins.
The first one is used to receive messages to decode/encode while the second one is used to send decoded/encoded messages further.
**Configuration should include at least one pin for each of the following sets of attributes:**
+ Pin for the stream encoding input: `encoder_in` `parsed` `subscribe`
+ Pin for the stream encoding output: `encoder_out` `raw` `publish`
+ Pin for the general encoding input: `general_encoder_in` `parsed` `subscribe`
+ Pin for the general encoding output: `general_encoder_out` `raw` `publish`
+ Pin for the stream decoding input: `decoder_in` `raw` `subscribe`
+ Pin for the stream decoding output: `decoder_out` `parsed` `publish`
+ Pin for the stream decoding input: `general_decoder_in` `raw` `subscribe`
+ Pin for the stream decoding output: `general_decoder_out` `parsed` `publish`

### Configuration example

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec-qfj
spec:
  image-name: ghcr.io/th2-net/th2-codec-qfj
  image-version: #lastVersion
  type: th2-codec
#  custom-config:
#    codecSettings:
#      fixt: true
#      checkFieldsOutOfOrder: true
#      replaceValuesWithEnumNames: false
  pins:
    # encoder
    - name: in_codec_encode
      connection-type: mq
      attributes: [ 'encoder_in', 'group', 'subscribe' ]
    - name: out_codec_encode
      connection-type: mq
      attributes: [ 'encoder_out', 'group', 'publish' ]
    # decoder
    - name: in_codec_decode
      connection-type: mq
      attributes: ['decoder_in', 'group', 'subscribe']
    - name: out_codec_decode
      connection-type: mq
      attributes: ['decoder_out', 'group', 'publish']
    # encoder general (technical)
    - name: in_codec_general_encode
      connection-type: mq
      attributes: ['general_encoder_in', 'group', 'subscribe']
    - name: out_codec_general_encode
      connection-type: mq
      attributes: ['general_encoder_out', 'group', 'publish']
    # decoder general (technical)
    - name: in_codec_general_decode
      connection-type: mq
      attributes: ['general_decoder_in', 'group', 'subscribe']
    - name: out_codec_general_decode
      connection-type: mq
      attributes: ['general_decoder_out', 'group', 'publish']
```
