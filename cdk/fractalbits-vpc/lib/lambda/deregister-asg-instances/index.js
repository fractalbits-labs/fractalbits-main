const {ServiceDiscoveryClient, DeregisterInstanceCommand, DiscoverInstancesCommand} = require('@aws-sdk/client-servicediscovery');
const servicediscovery = new ServiceDiscoveryClient({});

exports.handler = async (event, context) => {
  console.log('Event: ', JSON.stringify(event, null, 2));
  const serviceId = event.ResourceProperties.ServiceId;
  const namespaceName = event.ResourceProperties.NamespaceName;
  const serviceName = event.ResourceProperties.ServiceName;

  if (event.RequestType === 'Delete') {
    try {
      const discoverInstancesCommand = new DiscoverInstancesCommand({NamespaceName: namespaceName, ServiceName: serviceName});
      const instances = await servicediscovery.send(discoverInstancesCommand);

      for (const instance of instances.Instances) {
        const deregisterInstanceCommand = new DeregisterInstanceCommand({ServiceId: serviceId, InstanceId: instance.InstanceId});
        await servicediscovery.send(deregisterInstanceCommand);
        console.log(`Successfully deregistered instance: ${instance.InstanceId} from service: ${serviceId}`);
      }
    } catch (error) {
      console.error('Error deregistering instances:', error);
      // Don't fail the custom resource on error, as the service might already be gone
    }
  }

  return {Status: 'SUCCESS'};
};
