const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");

async function getPriorityMap() {
    const parameterName = process.env.PAPER_DELIVERY_PRIORITY_PARAMETER;
      const ssmClient = new SSMClient({ region: process.env.AWS_REGION });
    
      try {
        const command = new GetParameterCommand({
          Name: parameterName
        });
    
        const response = await ssmClient.send(command);
        return response?.Parameter?.Value || {};
      } catch (error) {
        console.error(`Error retrieving parameter ${parameterName}:`, error);
        throw error;
      }
};

module.exports = {
    getPriorityMap
};