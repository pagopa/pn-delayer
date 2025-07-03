function chunkArray(messages, size) {
    return Array.from({ length: Math.ceil(messages.length / size) },
    (_, i) => messages.slice(i * size, i * size + size));
}

const groupRecordsByProductAndProvince = (items) => {
  return items.reduce((acc, item) => {
    const { productType, province } = item.paperDeliveryIncoming;
    const key = `${productType}~${province}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(item);
    return acc;
  }, {});
};

module.exports = { chunkArray, groupRecordsByProductAndProvince };