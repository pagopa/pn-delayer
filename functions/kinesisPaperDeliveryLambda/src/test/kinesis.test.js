const { extractKinesisData } = require('../app/lib/kinesis');
const { expect } = require("chai");

describe('extractKinesisData', () => {
  it('returns an array of extracted body details from kinesisEvent', () => {
    const kinesisEvent = {"Records": [
      {
          "kinesis": {
              "kinesisSchemaVersion": "1.0",
              "partitionKey": "2e7b07db-d61b-1607-ab40-32f6fa53a1ba_31d06992-2834-64de-e464-10d68a860f0e",
              "sequenceNumber": "49662643059837983057243073891897044753573918713738952738",
              "data":"ewogICAgICAidmVyc2lvbiI6ICIwIiwKICAgICAgImlkIjogIjRlMTA5YzYzLTQ3NjMtNWFmNy1lMjU4LWQxNmJkMDEwMjIxNSIsCiAgICAgICJkZXRhaWwtdHlwZSI6ICJQcmVwYXJlUGhhc2VPbmVPdXRjb21lRXZlbnQiLAogICAgICAic291cmNlIjogIlBpcGUgcG4tcGFwZXItY2hhbm5lbC10by1kZWxheWVyLXBpcGUiLAogICAgICAiYWNjb3VudCI6ICI4MzAxOTIyNDY1NTMiLAogICAgICAidGltZSI6ICIyMDI1LTA0LTIzVDE0OjQ2OjEyWiIsCiAgICAgICJyZWdpb24iOiAiZXUtc291dGgtMSIsCiAgICAgICJyZXNvdXJjZXMiOiBbXSwKICAgICAgImRldGFpbCI6IHsKICAgICAgICAic291cmNlIjogInBuLXBhcGVyY2hhbm5lbF90b19kZWxheWVyIiwKICAgICAgICAiYm9keSI6IHsKICAgICAgICAgICJyZXF1ZXN0SWQiOiAiUFJFUEFSRV9BTkFMT0dfRE9NSUNJTEUuSVVOX01EWVItVVhITC1MVlJRLTIwMjUwNC1RLTEuUkVDSU5ERVhfMC5BVFRFTVBUXzAiLAogICAgICAgICAgInVuaWZpZWREZWxpdmVyeURyaXZlciI6ICJkcml2ZXIxIiwKICAgICAgICAgICJzZW5kZXJQYUlkIjogInBhSWQiLAogICAgICAgICAgInJlY2lwaWVudElkIjogInJlY2lwaWVudElkIiwKICAgICAgICAgICJ0ZW5kZXJJZCI6ICJ0ZW5kZXJJZCIsCiAgICAgICAgICAiaXVuIjogIk1EWVItVVhITC1MVlJRLTIwMjUwNC1RLTEiLAogICAgICAgICAgInJlY2lwaWVudE5vcm1hbGl6ZWRBZGRyZXNzIjogewogICAgICAgICAgICAiY2FwIjogIjg3MTAwIiwKICAgICAgICAgICAgInByIjogIkNTIgogICAgICAgICAgfQogICAgICAgIH0KICAgICAgfQogICAgfQ==",
              "approximateArrivalTimestamp": 1745419320.884
          },
          "eventSource": "aws:kinesis",
          "eventVersion": "1.0",
          "eventID": "shardId-000000000002:49662643059837983057243073891897044753573918713738952738",
          "eventName": "aws:kinesis:record",
          "invokeIdentityArn": "arn:aws:iam::830192246553:role/pn-kinesisPaperDeliveryLambdaRole",
          "awsRegion": "eu-south-1",
          "eventSourceARN": "arn:aws:kinesis:eu-south-1:830192246553:stream/pn-delayer_inputs"
      },
      {
          "kinesis": {
              "kinesisSchemaVersion": "1.0",
              "partitionKey": "4e109c63-4763-5af7-e258-d16bd0102215_8f7ab60d-2dc8-c92e-eb9a-828c3106592b",
              "sequenceNumber": "49662643059837983057243073898696043563086610578314100770",
              "data":"ewogICAgICAidmVyc2lvbiI6ICIwIiwKICAgICAgImlkIjogIjRlMTA5YzYzLTQ3NjMtNWFmNy1lMjU4LWQxNmJkMDEwMjIxNCIsCiAgICAgICJkZXRhaWwtdHlwZSI6ICJQcmVwYXJlUGhhc2VPbmVPdXRjb21lRXZlbnQiLAogICAgICAic291cmNlIjogIlBpcGUgcG4tcGFwZXItY2hhbm5lbC10by1kZWxheWVyLXBpcGUiLAogICAgICAiYWNjb3VudCI6ICI4MzAxOTIyNDY1NTMiLAogICAgICAidGltZSI6ICIyMDI1LTA0LTIzVDE0OjQ2OjEyWiIsCiAgICAgICJyZWdpb24iOiAiZXUtc291dGgtMSIsCiAgICAgICJyZXNvdXJjZXMiOiBbXSwKICAgICAgImRldGFpbCI6IHsKICAgICAgICAic291cmNlIjogInBuLXBhcGVyY2hhbm5lbF90b19kZWxheWVyIiwKICAgICAgICAiYm9keSI6IHsKICAgICAgICAgICJyZXF1ZXN0SWQiOiAiUFJFUEFSRV9BTkFMT0dfRE9NSUNJTEUuSVVOX01EWVItVVhITC1MVlJRLTIwMjUwNC1RLTEuUkVDSU5ERVhfMC5BVFRFTVBUXzEiLAogICAgICAgICAgInVuaWZpZWREZWxpdmVyeURyaXZlciI6ICJkcml2ZXIxIiwKICAgICAgICAgICJzZW5kZXJQYUlkIjogInBhSWQiLAogICAgICAgICAgInJlY2lwaWVudElkIjogInJlY2lwaWVudElkIiwKICAgICAgICAgICJ0ZW5kZXJJZCI6ICJ0ZW5kZXJJZCIsCiAgICAgICAgICAiaXVuIjogIk1EWVItVVhITC1MVlJRLTIwMjUwNC1RLTEiLAogICAgICAgICAgInJlY2lwaWVudE5vcm1hbGl6ZWRBZGRyZXNzIjogewogICAgICAgICAgICAiY2FwIjogIjg3MTAwIiwKICAgICAgICAgICAgInByIjogIkNTIgogICAgICAgICAgfQogICAgICAgIH0KICAgICAgfQogICAgfQ==",
              "approximateArrivalTimestamp": 1745419572.926
          },
          "eventSource": "aws:kinesis",
          "eventVersion": "1.0",
          "eventID": "shardId-000000000002:49662643059837983057243073898696043563086610578314100770",
          "eventName": "aws:kinesis:record",
          "invokeIdentityArn": "arn:aws:iam::830192246553:role/pn-kinesisPaperDeliveryLambdaRole",
          "awsRegion": "eu-south-1",
          "eventSourceARN": "arn:aws:kinesis:eu-south-1:830192246553:stream/pn-delayer_inputs"
      }]
    };
    const result = extractKinesisData(kinesisEvent);
    expect(result).to.deep.equal([
          {
            "iun": "MDYR-UXHL-LVRQ-202504-Q-1",
            "kinesisSeqNumber": "49662643059837983057243073891897044753573918713738952738",
            "recipientId": "recipientId",
            "recipientNormalizedAddress": {
              "cap": "87100",
              "pr": "CS"
            },
            "requestId": "PREPARE_ANALOG_DOMICILE.IUN_MDYR-UXHL-LVRQ-202504-Q-1.RECINDEX_0.ATTEMPT_0",
            "senderPaId": "paId",
            "tenderId": "tenderId",
            "unifiedDeliveryDriver": "driver1"
           },
           {
            "iun": "MDYR-UXHL-LVRQ-202504-Q-1",
            "kinesisSeqNumber": "49662643059837983057243073898696043563086610578314100770",
            "recipientId": "recipientId",
            "recipientNormalizedAddress": {
              "cap": "87100",
              "pr": "CS"
            },
            "requestId": "PREPARE_ANALOG_DOMICILE.IUN_MDYR-UXHL-LVRQ-202504-Q-1.RECINDEX_0.ATTEMPT_1",
            "senderPaId": "paId",
            "tenderId": "tenderId",
            "unifiedDeliveryDriver": "driver1"
           }
    ]);
  });

  it('returns an empty array when kinesisEvent is empty', () => {
    const kinesisEvent = [];
    const result = extractKinesisData(kinesisEvent);
    expect(result).to.be.undefined;
  });

  it('handles missing detail or body gracefully by returning a singleton list', () => {
    const kinesisEvent = {"Records": [
      {
          "kinesis": {}
      },
      {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "4e109c63-4763-5af7-e258-d16bd0102215_8f7ab60d-2dc8-c92e-eb9a-828c3106592b",
            "sequenceNumber": "49662643059837983057243073898696043563086610578314100770",
            "data":null,
            "approximateArrivalTimestamp": 1745419572.926
        },
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": "shardId-000000000002:49662643059837983057243073898696043563086610578314100770",
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": "arn:aws:iam::830192246553:role/pn-kinesisPaperDeliveryLambdaRole",
        "awsRegion": "eu-south-1",
        "eventSourceARN": "arn:aws:kinesis:eu-south-1:830192246553:stream/pn-delayer_inputs"
      },
      {
          "kinesis": {
              "kinesisSchemaVersion": "1.0",
              "partitionKey": "4e109c63-4763-5af7-e258-d16bd0102215_8f7ab60d-2dc8-c92e-eb9a-828c3106592b",
              "sequenceNumber": "49662643059837983057243073898696043563086610578314100770",
              "data":"ewogICAgICAidmVyc2lvbiI6ICIwIiwKICAgICAgImlkIjogIjRlMTA5YzYzLTQ3NjMtNWFmNy1lMjU4LWQxNmJkMDEwMjIxNCIsCiAgICAgICJkZXRhaWwtdHlwZSI6ICJQcmVwYXJlUGhhc2VPbmVPdXRjb21lRXZlbnQiLAogICAgICAic291cmNlIjogIlBpcGUgcG4tcGFwZXItY2hhbm5lbC10by1kZWxheWVyLXBpcGUiLAogICAgICAiYWNjb3VudCI6ICI4MzAxOTIyNDY1NTMiLAogICAgICAidGltZSI6ICIyMDI1LTA0LTIzVDE0OjQ2OjEyWiIsCiAgICAgICJyZWdpb24iOiAiZXUtc291dGgtMSIsCiAgICAgICJyZXNvdXJjZXMiOiBbXSwKICAgICAgImRldGFpbCI6IHsKICAgICAgICAic291cmNlIjogInBuLXBhcGVyY2hhbm5lbF90b19kZWxheWVyIiwKICAgICAgICAiYm9keSI6IHsKICAgICAgICAgICJyZXF1ZXN0SWQiOiAiUFJFUEFSRV9BTkFMT0dfRE9NSUNJTEUuSVVOX01EWVItVVhITC1MVlJRLTIwMjUwNC1RLTEuUkVDSU5ERVhfMC5BVFRFTVBUXzEiLAogICAgICAgICAgInVuaWZpZWREZWxpdmVyeURyaXZlciI6ICJkcml2ZXIxIiwKICAgICAgICAgICJzZW5kZXJQYUlkIjogInBhSWQiLAogICAgICAgICAgInJlY2lwaWVudElkIjogInJlY2lwaWVudElkIiwKICAgICAgICAgICJ0ZW5kZXJJZCI6ICJ0ZW5kZXJJZCIsCiAgICAgICAgICAiaXVuIjogIk1EWVItVVhITC1MVlJRLTIwMjUwNC1RLTEiLAogICAgICAgICAgInJlY2lwaWVudE5vcm1hbGl6ZWRBZGRyZXNzIjogewogICAgICAgICAgICAiY2FwIjogIjg3MTAwIiwKICAgICAgICAgICAgInByIjogIkNTIgogICAgICAgICAgfQogICAgICAgIH0KICAgICAgfQogICAgfQ==",
              "approximateArrivalTimestamp": 1745419572.926
          },
          "eventSource": "aws:kinesis",
          "eventVersion": "1.0",
          "eventID": "shardId-000000000002:49662643059837983057243073898696043563086610578314100770",
          "eventName": "aws:kinesis:record",
          "invokeIdentityArn": "arn:aws:iam::830192246553:role/pn-kinesisPaperDeliveryLambdaRole",
          "awsRegion": "eu-south-1",
          "eventSourceARN": "arn:aws:kinesis:eu-south-1:830192246553:stream/pn-delayer_inputs"
    }]};
    const result = extractKinesisData(kinesisEvent);
    expect(result).to.deep.equal([
      {
        "iun": "MDYR-UXHL-LVRQ-202504-Q-1",
        "kinesisSeqNumber": "49662643059837983057243073898696043563086610578314100770",
        "recipientId": "recipientId",
        "recipientNormalizedAddress": {
          "cap": "87100",
          "pr": "CS"
        },
        "requestId": "PREPARE_ANALOG_DOMICILE.IUN_MDYR-UXHL-LVRQ-202504-Q-1.RECINDEX_0.ATTEMPT_1",
        "senderPaId": "paId",
        "tenderId": "tenderId",
        "unifiedDeliveryDriver": "driver1"
      }
    ]);
  });
});