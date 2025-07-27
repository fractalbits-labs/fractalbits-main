const {ServiceDiscoveryClient, DeregisterInstanceCommand, ListInstancesCommand} = require('@aws-sdk/client-servicediscovery');
const servicediscovery = new ServiceDiscoveryClient({});

exports.handler = async (event, context) => {
  console.log('Event: ', JSON.stringify(event, null, 2));
  const serviceName = event.ResourceProperties.ServiceName;
  const serviceId = event.ResourceProperties.ServiceId;

  if (event.RequestType === 'Delete') {
    try {
      const listInstancesCommand = new ListInstancesCommand({ServiceId: serviceId});
      console.log('Listing instances from:', serviceName, serviceId);
      const instances = await servicediscovery.send(listInstancesCommand);

      console.log('Instances: ', JSON.stringify(instances.Instances, null, 2));
      for (const instance of instances.Instances) {
        const deregisterInstanceCommand = new DeregisterInstanceCommand({ServiceId: serviceId, InstanceId: instance.Id});
        await servicediscovery.send(deregisterInstanceCommand);
        console.log(`Successfully deregistered instance: ${instance.Id} from service: ${serviceId}`);
      }
    } catch (error) {
      console.error('Error deregistering instances:', error);
      // Don't fail the custom resource on error, as the service might already be gone
    }
  }

  return {Status: 'SUCCESS'};
};
