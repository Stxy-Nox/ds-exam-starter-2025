import { APIGatewayProxyHandlerV2 } from "aws-lambda";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const client = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
   
    if ( event.pathParameters) {
      const { role, movieId } = event.pathParameters;
      
      if (role && movieId) {
       
        const params = {
          TableName: process.env.TABLE_NAME,
          Key: {
            movieId: parseInt(movieId),
            role: role
          }
        };
        
        const response = await client.send(new GetCommand(params));
        
   
        if (response.Item) {
          return {
            statusCode: 200,
            headers: {
              "content-type": "application/json",
            },
            body: JSON.stringify(response.Item),
          };
        } else {
         
          return {
            statusCode: 404,
            headers: {
              "content-type": "application/json",
            },
            body: JSON.stringify({ message: `can't find ID ${movieId} 中角色为 ${role} 的成员信息` }),
          };
        }
      }
    }

    return {
      statusCode: 400,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ message: "request fromat incorrect" }),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
