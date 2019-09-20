from kubernetes import client, config

# Configs can be set in Configuration class directly or using helper utility
config.load_kube_config('./config')

v1 = client.CoreV1Api()
#print("Listing pods with their IPs:")
#ret = v1.list_pod_for_all_namespaces(watch=False)
#for i in ret.items:
#    print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

#for ns in v1.list_namespace(watch=False).items:
#    print ns.metadata.name

#for pvc in v1.list_namespaced_persistent_volume_claim(namespace='rook-ceph').items:
#    print pvc
for i in v1.list_namespaced_persistent_volume_claim(namespace='wordpress-004').items:
     a=i.spec.volume_name
#print v1.list_namespaced_persistent_volume_claim(namespace='wordpress-004').spec.volume_name

#print a
for i in v1.list_persistent_volume().items:
    if i.metadata.name == a:
        image = i.spec.rbd.image
        break

print image



