# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: cclob.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0b\x63\x63lob.proto\x12\x07message\"=\n\tMsgHeader\x12\r\n\x05\x62\x61tch\x18\x01 \x01(\x03\x12\x11\n\ttimestamp\x18\x02 \x01(\x03\x12\x0e\n\x06symbol\x18\x03 \x01(\t\"\x88\x01\n\x07\x43\x43Order\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\x12\x0c\n\x04side\x18\x02 \x01(\t\x12\x0c\n\x04type\x18\x03 \x01(\t\x12\r\n\x05level\x18\x04 \x01(\x05\x12\r\n\x05price\x18\x05 \x01(\x01\x12\x0e\n\x06volume\x18\x06 \x01(\x01\x12\x0e\n\x06\x61mount\x18\x07 \x01(\x01\x12\x10\n\x08midprice\x18\x08 \x01(\x01\"z\n\x07\x43\x43Trade\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\x12\x0c\n\x04side\x18\x02 \x01(\t\x12\r\n\x05level\x18\x03 \x01(\x05\x12\r\n\x05price\x18\x04 \x01(\x01\x12\x0e\n\x06volume\x18\x05 \x01(\x01\x12\x0e\n\x06\x61mount\x18\x06 \x01(\x01\x12\x10\n\x08midprice\x18\x07 \x01(\x01\"s\n\x05\x43\x43\x42\x61r\x12\x0c\n\x04open\x18\x01 \x01(\x01\x12\x0c\n\x04high\x18\x02 \x01(\x01\x12\x0b\n\x03low\x18\x03 \x01(\x01\x12\r\n\x05\x63lose\x18\x04 \x01(\x01\x12\x0e\n\x06volume\x18\x05 \x01(\x01\x12\x10\n\x08turnover\x18\x06 \x01(\x01\x12\x10\n\x08tradenum\x18\x07 \x01(\x05\"R\n\x0f\x43\x43LevelColumnar\x12\x11\n\ttimestamp\x18\x01 \x03(\x03\x12\r\n\x05level\x18\x02 \x03(\x05\x12\r\n\x05price\x18\x03 \x03(\x01\x12\x0e\n\x06volume\x18\x04 \x03(\x01\"~\n\nCCLevelMsg\x12\"\n\x06header\x18\x01 \x01(\x0b\x32\x12.message.MsgHeader\x12%\n\x03\x62id\x18\x02 \x01(\x0b\x32\x18.message.CCLevelColumnar\x12%\n\x03\x61sk\x18\x03 \x01(\x0b\x32\x18.message.CCLevelColumnar\"Q\n\nCCOrderMsg\x12\"\n\x06header\x18\x01 \x01(\x0b\x32\x12.message.MsgHeader\x12\x1f\n\x05order\x18\x02 \x03(\x0b\x32\x10.message.CCOrder\"Q\n\nCCTradeMsg\x12\"\n\x06header\x18\x01 \x01(\x0b\x32\x12.message.MsgHeader\x12\x1f\n\x05trade\x18\x02 \x03(\x0b\x32\x10.message.CCTrade\"Y\n\x08\x43\x43\x42\x61rMsg\x12\"\n\x06header\x18\x01 \x01(\x0b\x32\x12.message.MsgHeader\x12\x0c\n\x04type\x18\x02 \x01(\t\x12\x1b\n\x03\x62\x61r\x18\x03 \x01(\x0b\x32\x0e.message.CCBarb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'cclob_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _MSGHEADER._serialized_start=24
  _MSGHEADER._serialized_end=85
  _CCORDER._serialized_start=88
  _CCORDER._serialized_end=224
  _CCTRADE._serialized_start=226
  _CCTRADE._serialized_end=348
  _CCBAR._serialized_start=350
  _CCBAR._serialized_end=465
  _CCLEVELCOLUMNAR._serialized_start=467
  _CCLEVELCOLUMNAR._serialized_end=549
  _CCLEVELMSG._serialized_start=551
  _CCLEVELMSG._serialized_end=677
  _CCORDERMSG._serialized_start=679
  _CCORDERMSG._serialized_end=760
  _CCTRADEMSG._serialized_start=762
  _CCTRADEMSG._serialized_end=843
  _CCBARMSG._serialized_start=845
  _CCBARMSG._serialized_end=934
# @@protoc_insertion_point(module_scope)
