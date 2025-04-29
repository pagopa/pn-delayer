exports.extractKinesisData = function (kinesisEvent) {
  return kinesisEvent.filter((rec) => rec?.detail?.body).map((rec) => {
    return {
      ...rec.detail.body,
    };
  });
};
