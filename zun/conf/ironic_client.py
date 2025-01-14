# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_config import cfg


ironic_group = cfg.OptGroup(name='ironic_client',
                             title='Options for the Ironic client')

common_security_opts = [
    cfg.StrOpt('ca_file',
               help='Optional CA cert file to use in SSL connections.'),
    cfg.StrOpt('cert_file',
               help='Optional PEM-formatted certificate chain file.'),
    cfg.StrOpt('key_file',
               help='Optional PEM-formatted file that contains the '
                    'private key.'),
    cfg.BoolOpt('insecure',
                default=False,
                help="If set, then the server's certificate will not "
                     "be verified.")]

ironic_client_opts = [
    cfg.StrOpt(
        'ironic_api_version',
        default='1',
        help='Ironic API version'),
    cfg.StrOpt(
        'ironic_api_microversion',
        default='1.31',
        help='Ironic API microversion'),
    cfg.StrOpt('region_name',
               help='Region in Identity service catalog to use for '
                    'communication with the OpenStack service.'),
    cfg.StrOpt('endpoint_type',
               default='publicURL',
               help='Type of endpoint in Identity service catalog to use '
                    'for communication with the OpenStack service.')]


ALL_OPTS = (ironic_client_opts + common_security_opts)


def register_opts(conf):
    conf.register_group(ironic_group)
    conf.register_opts(ALL_OPTS, group=ironic_group)


def list_opts():
    return {ironic_group: ALL_OPTS}
