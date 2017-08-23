import boto3
import datetime as dt
import time
from botocore.exceptions import ClientError
from common_functions import *

ec2 = boto3.resource('ec2', region_name='us-east-1')

client = boto3.client('ec2', region_name='us-east-1')


def get_instances():
    return sorted(list(ec2.instances.all()), key=lambda x: x.launch_time)


def associate_address(new=True):
    instances = get_instances()
    instance_id = get_instances()[0].id
    if new:
        instance_id = get_instances()[-1].id
    AllocationId = 'eipalloc-6bd3155b'
    print(instances[-1].id)

    return client.associate_address(
        AllocationId=AllocationId,
        InstanceId=instances[-1].id,
        # PublicIp='string',
        AllowReassociation=True,
        DryRun=False,
        NetworkInterfaceId='eni-c93eb514', )


# owner_id = boto3.client('sts').get_caller_identity().get('Account')
# filters = [{'Name': owner_id, 'Values': [owner_id]}]
client_token = ''
instances = get_instances()
print('instances: ')
for instance in instances:
    # print(instance)
    print(instance.id, instance.client_token, instance.launch_time)

print()


def get_images():
    images = list(ec2.images.filter(Owners=['self']).all())
    images = sorted(images, key=lambda x: x.creation_date, reverse=True)
    return images


def newest_image():
    return get_images()[0]


# config = {
#     "IamFleetRole": "arn:aws:iam::298855296140:role/aws-ec2-spot-fleet-role",
#     "AllocationStrategy": "lowestPrice",
#     "TargetCapacity": 1,
#     "SpotPrice": "0.398",
#     "ValidFrom": dt.datetime.today().date(),
#     "ValidUntil": dt.datetime.today().date(),
#     "TerminateInstancesWithExpiration": True,
#     "LaunchSpecifications": [
#         {
#             "ImageId": newest_image().id,
#             "InstanceType": "c4.xlarge",
#             "SubnetId": "subnet-3b7b1562",
#             "KeyName": "id_rsa",
#             "SpotPrice": "0.398",
#             "BlockDeviceMappings": [
#                 {
#                     "DeviceName": "/dev/sda1",
#                     "Ebs": {
#                         "DeleteOnTermination": True,
#                         "VolumeType": "gp2",
#                         "VolumeSize": 8,
#                         "SnapshotId": "snap-c1afae2b"
#                     }
#                 }
#             ],
#             "SecurityGroups": [
#                 {
#                     "GroupId": "sg-66888202"
#                 }
#             ]
#         }
#     ],
#     "Type": "maintain"
# }

config = {
    'SecurityGroupIds': [
        'sg-66888202',
    ],
    # 'SecurityGroups': [
    #     'string',
    # ],
    # 'AddressingType': 'string',
    'BlockDeviceMappings': [
        {
            "DeviceName": "/dev/sda1",
            # 'VirtualName': 'string',
            'Ebs': {
                # 'Encrypted': True|False,
                # 'DeleteOnTermination': True,
                # 'Iops': 123,
                # 'SnapshotId': 'string',
                # 'VolumeSize': 123,
                # 'VolumeType': 'standard'|'io1'|'gp2'|'sc1'|'st1'
                "DeleteOnTermination": True,
                "VolumeType": "gp2",
                "VolumeSize": 8,
                # "SnapshotId": "snap-c1afae2b"
            },
            # 'NoDevice': 'string'
        },
    ],
    'EbsOptimized': False,
    # 'IamInstanceProfile': {
    #     'Arn': 'string',
    #     'Name': 'string'
    # },
    # 'ImageId': 'string',
    # 'InstanceType': 't1.micro' | 't2.nano' | 't2.micro' | 't2.small' | 't2.medium' | 't2.large' | 't2.xlarge' | 't2.2xlarge' | 'm1.small' | 'm1.medium' | 'm1.large' | 'm1.xlarge' | 'm3.medium' | 'm3.large' | 'm3.xlarge' | 'm3.2xlarge' | 'm4.large' | 'm4.xlarge' | 'm4.2xlarge' | 'm4.4xlarge' | 'm4.10xlarge' | 'm4.16xlarge' | 'm2.xlarge' | 'm2.2xlarge' | 'm2.4xlarge' | 'cr1.8xlarge' | 'r3.large' | 'r3.xlarge' | 'r3.2xlarge' | 'r3.4xlarge' | 'r3.8xlarge' | 'r4.large' | 'r4.xlarge' | 'r4.2xlarge' | 'r4.4xlarge' | 'r4.8xlarge' | 'r4.16xlarge' | 'x1.16xlarge' | 'x1.32xlarge' | 'i2.xlarge' | 'i2.2xlarge' | 'i2.4xlarge' | 'i2.8xlarge' | 'i3.large' | 'i3.xlarge' | 'i3.2xlarge' | 'i3.4xlarge' | 'i3.8xlarge' | 'i3.16xlarge' | 'hi1.4xlarge' | 'hs1.8xlarge' | 'c1.medium' | 'c1.xlarge' | 'c3.large' | 'c3.xlarge' | 'c3.2xlarge' | 'c3.4xlarge' | 'c3.8xlarge' | 'c4.large' | 'c4.xlarge' | 'c4.2xlarge' | 'c4.4xlarge' | 'c4.8xlarge' | 'cc1.4xlarge' | 'cc2.8xlarge' | 'g2.2xlarge' | 'g2.8xlarge' | 'g3.4xlarge' | 'g3.8xlarge' | 'g3.16xlarge' | 'cg1.4xlarge' | 'p2.xlarge' | 'p2.8xlarge' | 'p2.16xlarge' | 'd2.xlarge' | 'd2.2xlarge' | 'd2.4xlarge' | 'd2.8xlarge' | 'f1.2xlarge' | 'f1.16xlarge',
    # 'KernelId': 'string',
    # 'KeyName': 'string',
    "ImageId": newest_image().id,
    "InstanceType": "c4.xlarge",
    # "InstanceType": "t2.small",
    # "InstanceType": "c3.large",
    "SubnetId": "subnet-3b7b1562",
    "KeyName": "id_rsa",
    # "SpotPrice": "0.398",
    'Monitoring': {
        'Enabled': False
    },
    # 'NetworkInterfaces': [
    #     {
    #         'AssociatePublicIpAddress': True | False,
    #         'DeleteOnTermination': True | False,
    #         'Description': 'string',
    #         'DeviceIndex': 123,
    #         'Groups': [
    #             'string',
    #         ],
    #         'Ipv6AddressCount': 123,
    #         'Ipv6Addresses': [
    #             {
    #                 'Ipv6Address': 'string'
    #             },
    #         ],
    #         'NetworkInterfaceId': 'string',
    #         'PrivateIpAddress': 'string',
    #         'PrivateIpAddresses': [
    #             {
    #                 'Primary': True | False,
    #                 'PrivateIpAddress': 'string'
    #             },
    #         ],
    #         'SecondaryPrivateIpAddressCount': 123,
    #         'SubnetId': 'string'
    #     },
    # ],
    'Placement': {
        'AvailabilityZone': 'us-east-1a',
        'GroupName': 'stock',
        'Tenancy': 'default'
    },
    # 'RamdiskId': 'string',
    'SubnetId': 'subnet-3b7b1562',
    # 'UserData': 'string'
}

image_config = {
    'DeviceName': "/dev/sda1",
    # 'VirtualName': 'string',
    'Ebs': {
        'Encrypted': False,
        'DeleteOnTermination': False,
        # 'Iops': 123,
        # 'SnapshotId': 'string',
        # 'VolumeSize': 123,
        'VolumeType': 'gp2',
    },
    # 'NoDevice': 'string'
}


def request_spot_instance():
    global instances
    instances = get_instances()
    if len(list(instances)) < 2:
        try:
            response = client.request_spot_instances(
                AvailabilityZoneGroup='us-east-1a',
                # BlockDurationMinutes=60 * 1,
                ClientToken=dt.datetime.now().isoformat(),
                DryRun=False,
                InstanceCount=1,
                # LaunchGroup='string',
                LaunchSpecification=config,
                SpotPrice='0.5',
                Type='one-time',
                # ValidFrom=dt.datetime(2017, 7, 17),
                # ValidUntil=dt.datetime(2018, 1, 1),
                ValidFrom=dt.datetime.utcnow() + dt.timedelta(seconds=2),
                ValidUntil=dt.datetime.utcnow() + dt.timedelta(minutes=10),
            )
        except Exception as e:
            print(e)
            raise
        else:
            print(response)

        while len(get_instances()) < 2:
            time.sleep(2)
    time.sleep(30)

    associate_address()


def create_small_instance():
    ec2.create_instances(
        # AvailabilityZoneGroup='us-east-1a',
        ImageId=newest_image().id,
        InstanceType='t2.small',
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[
            'sg-66888202',
        ],
        SubnetId="subnet-3b7b1562",
        KeyName='id_rsa',
        # IamInstanceProfile={
        #     'Arn': 'arn:aws:iam::123456789012:instanceprofile/ExampleInstanceProfile',
        #     'Name': 'ExampleInstanceProfile'
        # }
        # DryRun=True,
    )
    time.sleep(60)
    associate_address()


if __name__ == '__main__':
    count_running = 0
    for instance in instances:
        print(instance.state)
        if instance.state.get('Name') == 'running':
            count_running += 1
    # create_instance()
    exit(0)

    if count_running == 1:
        color_print('Requesting spot instance!', Color.HEADER)
        # request_spot_instance()
    else:
        server_instance = None
        # locate the server instance
        for instance in instances:
            if instance.public_ip_address == "34.226.44.46":
                server_instance = instance

        if server_instance:
            color_print('Creating image for spot server instance!', Color.HEADER)
            name = str(dt.datetime.now()).replace(':', '.')
            color_print(f'Image name is {name}', Color.OKGREEN)
            color_print(server_instance.public_ip_address, Color.OKBLUE)
            try:
                image = server_instance.create_image(
                    BlockDeviceMappings=[
                        image_config,
                    ],
                    Description=name,
                    # DryRun=True,
                    Name=name,
                )
            except Exception as e:
                print(e)
                raise
            else:
                while image.state == 'pending':
                    time.sleep(5)
                    image.reload()

                color_print(f'Image {name} state: {image.state}', Color.OKGREEN)
                instances = get_instances()

                if image.state == 'available':
                    color_print(f'Image {name} created', Color.OKGREEN)

                    create_small_instance()
                    color_print(f'Terminating all the instances...', Color.OKBLUE)
                    ec2.instances.filter(InstanceIds=[instance.id for instance in instances]).terminate()
                    time.sleep(30)

                else:
                    color_print(f'Image {name} cannot be created!', Color.FAIL)
        else:
            color_print(f'Error, no server instance!', Color.FAIL)

            color_print(f'Creating new instance!', Color.HEADER)
            create_small_instance()
