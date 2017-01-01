import os
import re
import subprocess

import click

from . import constants


def looks_like_ip(potential_ip):
  """Check if a potential IP address looks like an IPv4 address"""
  return re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d{1,5})?$', potential_ip)


@click.group()
def cli():
  pass


@cli.group('consul')
def consul_group():
  """Commands for dealing with the consul box"""
  pass


@consul_group.command('create')
@click.option('--instance-name', '-n', default=constants.DEFAULT_CONSUL_BOX_NAME,
              help="The name to give the Consul instance")
@click.option('--instance-type', '-t', default='t2.nano',
              help="The type of instance to use for brining up the Consul box")
@click.option('--security-group', '-g', default=constants.DEFAULT_CONSUL_SECURITY_GROUP,
              help=('The security group to use for the Consul box. Defaults to ' +
                    constants.DEFAULT_SECURITY_GROUP))
def create_consul_box(instance_name, instance_type, security_group):
  """Create a consul box for use as service discovery. Only necessary once. Should just leave up
  permanently"""
  driver_options = ['--driver', 'amazonec2', '--amazonec2-security-group=' + security_group]
  subprocess.check_call(['docker-machine', 'create'
                         '--driver', 'amazonec2',
                         '--amazonec2-security-group=' + security_group,
                         '--amazonec2-instance-type=' + instance_type,
                         instance_name])
  machine_connection = subprocess.check_output(['docker-machine', 'config', instance_name])
  subprocess.check_call(['docker', machine_connection, 'run', '-d',
                         '-p', '8400:8400', '-p', '8500:8500/tcp', '-p', '8600:53/udp',
                         '-e', 'CONSUL_LOCAL_CONFIG={"acl_datacenter":"dc1",'
                               '"acl_default_policy":"deny","acl_down_policy":"extend-cache",'
                               '"acl_master_token":"the_one_ring","bootstrap_expect":1,'
                               '"datacenter":"dc1","data_dir":"/usr/local/bin/consul.d/data",'
                               '"server":true}',
                         'consul:0.7.2', 'agent', '-server', '-bind=127.0.0.1', '-client=0.0.0.0'])
  click.echo("Consul box successfully created at " + machine_connection)


@consul_group.command('destroy')
@click.option('--instance-name', '-n', default=constants.DEFAULT_CONSUL_BOX_NAME
              help='The name of the box to tear down')
def destroy_consul_box(instance_name):
  """Destroy the Consul box"""
  subprocess.check_call(['docker-machine', 'rm', '-y', instance_name])


@cli.command('create')
@click.argument('num_workers')
@click.option('--cluster-prefix', '-p', default=constants.DEFAULT_SPARK_CLUSTER_PREFIX,
              help="The prefix with which to name the cluster.")
@click.option('--security-group', '-g', default=constants.DEFAULT_SECURITY_GROUP,
              help="The security group our cluster will inhabit")
@click.option("--consul", '-c', default=constants.DEFAULT_CONSUL_BOX_NAME,
              help="The name or IP of the Consul box.")
@click.option('--network-interface', '-i', default='eth0',
              help="The network interface on which service discovery occurs. Default is eth0.")
@click.option('--master-instance-type', '-m', default='m4.large',
              help="The instance type of the master node. Default is m4.large")
@click.option('--worker-spot-price', '-s', default='0.074',
              help="The spot price we'll pay for our worker machines")
@click.option('--worker-instance-type', '-w', default='m4.2xlarge',
              help='The instance type used for Spark workers')
@click.option('--docker-compose-file', '-f', default=None,
              help="The location of the docker-compose.yml file you want to use")
def create_cluster(num_workers, cluster_prefix, security_group, consul, network_interface,
                   worker_spot_price, worker_instance_type, docker_compose_file):
  """Create a spark cluster with the specified number of workers"""
  click.echo("Creating a spark cluster with {} workers...".format(num_workers))

  if not looks_like_ip(consul):
    consul = get_consul_ip(consul)
    click.echo("Found Consul at " + consul)

  if not docker_compose_file:
    docker_compose_file = os.path.join(os.path.dirname(__file__, 'docker-compose.yml'))
    click.echo("Found docker-compose.yml at " + docker_compose_file)

  driver_options = ['--driver', 'amazonec2', '--amazonec2-security-group=' + security_group]
  swarm_options = ['--swarm', '--swarm-discovery=consul://{}:8500'.format(consul),
                   '--engine-opt=cluster-store=consul://{}:8500'.format(consul),
                   '--engine-opt=cluster-advertise={}:2376'.format(network_interface)]

  master_name = cluster_prefix + '-master'
  subprocess.check_call(['docker-machine', 'create'] + driver_options + swarm_options +
                        ['--swarm-master', '-engine-label', 'role=master',
                         '--amazonec2-instance-type=' + master_instance_type,
                         master_name])

  swarm_env = get_swarm_env(master_name)
  subprocess.check_call(['docker-compose', '-f', docker_compose_file,
                         'up', '-d', 'master'], env=swarm_env)
  master_dns_name = get_ip_from_name(master_name, private=False)
  click.echo("Master node up. You can test by going to http://{}:8080".format(master_dns_name))

  click.echo("Bringing up {} workers...".format(num_workers))
  worker_prefix = cluster_prefix + '-worker-'
  processes = [
    subprocess.Popen(['docker-machine', 'create'] + driver_options + swarm_options +
                     ['--amazonec2-request-spot-instance',
                      '--amazonec2-spot=price=' + worker_spot_price,
                      '--amazonec2-instance-type=' + worker_instance_type,
                      worker_prefix + str(worker_num)])
    for worker_num in range(num_workers)]

  for p in processes:
    p.wait()

  click.echo("Adding workers to swarm...")
  subprocess.check_call(['docker-compose', '-f' docker_compose_file,
                         'scale', 'master=1', 'worker=10'], env=swarm_env)


def get_swarm_env(master_name):
  """Return the environment variables to be used for the given swarm.

  :return: The environment variables
  :rtype: dict[str, str]
  """
  raw_env = subprocess.check_output(['docker-machine', 'env',
                                     '--shell', 'sh',
                                     '--swarm', master_name])
  env_vars.update(os.environ.copy())
  env_vars.update(dict(x.split('=', 1) for x in re.finall(r'export ([A-Z_]*=[^\n\m\r]*)', raw_env)))


def get_ip_from_name(instance_name, private=True):
  """Return the private IP address or public DNS of the instance with the given name

  :pararm str instance_name: The name of the instance
  :param bool private: If True, return private IP address, else return the public DNS
  :return: The private IP address of the instance or the public DNS of the instance
  :rtype: str
  :raises EnvironmentError: When the instance isn't running
  """
  raw_json = subprocess.check_output(['aws', 'ec2', 'describe-instances'])
  j = json.loads(raw_json)
  instance = j['Reservations']['Instances'][instance_name]
  if instance ['State']['Name'] != 'running':
    raise EnvironmentError("Instance with name {} is not currently in a running state".format(instance_name))
  if private:
    return instance['PrivateIpAddress']
  return instance['PublicDnsName']


@cli.command('test')
@click.option('--cluster-prefix', '-p', default=constants.DEFAULT_SPARK_CLUSTER_PREFIX,
              help="The prefix with which to name the cluster.")
def test_spark_cluster(cluster_prefix):
  """Compute a few digits of Pi to test whether the Spark cluster is setup correctly"""
  master_name = cluster_prefix + '-master'
  swarm_env = get_swarm_env(master_name)

  click.echo("Submitting job to " + master_name)
  output = subprocess.check_output(['docker', 'run',
    '--net=container:master', '--entrypoint' 'spark-submit',
    'gettyimages/spark:2.0.2-hadoop-2.7',
    '--master', 'spark://master:7077',
    '--class', 'org.apache.spark.examples.SparkPi',
    '/usr/spark/lib/spark-examples-2.0.2-hadoop2.7.0.jar'])
  click.echo(output)


if __name__ == '__main__':
  cli()
