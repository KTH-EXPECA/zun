# Copyright 2017 IBM Corp
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from collections import defaultdict
import errno
import re

from oslo_log import log as logging
from zun.common import exception
from zun.common import utils
from zun.container.os_capability import host_capability


LOG = logging.getLogger(__name__)


class LinuxHost(host_capability.Host):

    def get_cpu_numa_info(self):
        # TODO(sbiswas7): rootwrap changes for zun required.
        old_lscpu = False
        try:
            output = utils.execute('lscpu', '-p=socket,cpu,online')
        except exception.CommandError:
            LOG.info("There was a problem while executing lscpu -p=socket"
                     ",cpu,online. Try again without the online column.")
            # There is a possibility that an older version of lscpu is used
            # So let's try without the online column
            output = utils.execute('lscpu', '-p=socket,cpu')
            old_lscpu = True

        if old_lscpu:
            cpu_sock_pair = re.findall(r"\d+(?:,\d+)?", str(output))
        else:
            cpu_sock_pair = re.findall(r"\d+(?:,\d+,[Y/N])?", str(output))
        sock_map = defaultdict(list)
        for value in cpu_sock_pair:
            val = value.split(",")
            if len(val) == 3 and val[2] == 'Y':
                sock_map[val[0]].append(int(val[1]))
            elif len(val) == 2 and old_lscpu:
                sock_map[val[0]].append(int(val[1]))
        return sock_map

    def get_mem_numa_info(self):
        try:
            output = utils.execute('numactl', '-H')
        except OSError as e:
            if e.errno == errno.ENOENT:
                LOG.info("The program 'numactl' is not installed.")
                return []
            else:
                raise
        except exception.CommandError as e:
            stdout = e.kwargs.get("stdout")
            if stdout and "No NUMA available on this system" in stdout:
                # Fake a single NUMA cell that uses all total available memory.
                _, _, mem_ava, _ = self.get_host_mem()
                return [mem_ava]
            else:
                raise

        sizes = re.findall(r"size\: \d*", str(output))
        mem_numa = []
        for size in sizes:
            mem_numa.append(int(size.split(' ')[1]))
        return mem_numa
