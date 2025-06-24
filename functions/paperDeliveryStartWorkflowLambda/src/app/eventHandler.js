const { getDeliveryWeek } = require('./lib/utils');
const { retrieveProvinceWithPaperDeliveries } = require('./lib/dynamo');

exports.handleEvent = async () => {
  const deliveryWeek = getDeliveryWeek();
  const items = await retrieveProvinceWithPaperDeliveries(deliveryWeek);

  const provinces = (items || [])
    .map(item => item.sk)
    .filter(sk => sk && sk.startsWith('EVAL~'))
    .map(sk => sk.replace('EVAL~', ''));

  const response = {
    deliveryWeek,
    provinces
  };

  console.log('Responses:', JSON.stringify(response));
  return response;
};