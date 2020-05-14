import pymysql
import datetime
import base64
import json
import xlrd
import sys
import os


filedir = 'RVTOOL'

database = json.loads(base64.b64decode('***'))


sheets = {
    'vInfo': {
        'f' : ['CBT', 'Cluster rule name(s)', 'Unshared MB', 'Provisioned MB', 'Template', 'HW version', 'In Use MB', 'DAS protection', 'CPUs', 'Boot Required', 'Video Ram KB', 'HW upgrade status', 'Snapshot directory', 'Consolidation Needed', 'Log directory', 'VI SDK Server', 'Network #3', 'FT Sec. Latency', 'Num Monitors', 'FT Bandwidth', 'vApp', 'VM', 'OS according to the VMware Tools', 'DNS Name', 'HA Isolation Response', 'Guest state', 'Powerstate', 'Creation date', 'Latency Sensitivity', 'Connection state', 'Network #1', 'OS according to the configuration file', 'Boot delay', 'Boot BIOS setup', 'Suspend time', 'Firmware', 'VM ID', 'FT Latency', 'VI SDK API Version', 'Primary IP Address', 'Heartbeat', 'HA Restart Priority', 'Annotation', 'HW target', 'NICs', 'Resource pool', 'Host', 'Boot retry enabled', 'Datacenter', 'Boot retry delay', 'VM UUID', 'Network #4', 'Config status', 'Suspend directory', 'Config Checksum', 'VI SDK Server type', 'HA VM Monitoring', 'Change Version', 'Folder', 'Path', 'Memory', 'EnableUUID', 'Cluster rule(s)', 'HW upgrade policy', 'Disks', 'Cluster', 'FT State', 'VI SDK UUID', 'Network #2', 'PowerOn'],
        'r' : []
    },

    'vHost':{
        'f' :  ['# VMs', 'Console', 'Service tag', 'Domain', 'GMT Offset', 'VM Memory Ballooned', 'VM Memory Swapped', '# CPU', 'Host Power Policy', 'OEM specific string', 'vRAM', 'BIOS Vendor', 'Memory usage %', '# HBAs', 'VI SDK Server', 'VMs per Core', '# vCPUs', 'NTPD running', 'BIOS Version', 'CPU Model', '# Memory', 'ESX Version', 'Object ID', 'VM Used memory', '# NICs', 'Storage VMotion support', 'Supported CPU power man.', 'Serial number', 'Time Zone', 'ATS Heartbeat', 'DNS Servers', 'Max EVC', 'Assigned License(s)', 'Vendor', 'DHCP', 'Cores per CPU', 'Current EVC', 'Host', 'Datacenter', 'HT Available', 'Speed', 'Config status', 'vCPUs per Core', 'VMotion support', 'DNS Search Order', 'NTP Server(s)', 'Time Zone Name', 'CPU usage %', 'Model', 'Boot time', 'Current CPU power man. policy', 'Cluster', 'BIOS Date', '# Cores', 'HT Active', 'VI SDK UUID', 'ATS Locking'],
        'r': []
    },

    'vDatastore':{
        'f' : ['# VMs', 'SIOC Threshold', 'Provisioned MB', 'Name', 'In Use MB', 'Capacity MB', '# Hosts', '# Extents', 'VI SDK Server', 'Address', 'URL', 'Cluster capacity MB', 'Major Version', 'Hosts', 'VMFS Upgradeable', 'SIOC enabled', 'Block size', 'Max Blocks', 'Accessible', 'Config status', 'Cluster name', 'Version', 'MHA', 'Type', 'Free MB', 'Free %', 'Cluster free space MB', 'VI SDK UUID'],
        'r' : []
    },

    'dvPort':{
        'f' : ['Blocked', 'Active Uplink', 'In Avg', 'In Traffic Shaping', 'Teaming Override', 'Reverse Policy', 'Policy', 'Live Port Moving', 'VI SDK Server', 'Out Traffic Shaping', 'In Peak', 'Out Avg', 'Percentage', '# Ports', 'Check Speed', 'Rolling Order', 'Check Duplex', 'Vlan Override', 'Switch', 'Block Override', 'In Burst', 'VLAN', 'Port', 'Sec. Policy Override', 'Config Reset', 'Shaping Override', 'Full Duplex', 'Allow Promiscuous', 'Check Error %', 'Speed', 'Mac Changes', 'Notify Switch', 'Out Burst', 'Standby Uplink', 'Check Beacon', 'Type', 'Vendor Config Override', 'Forged Transmits', 'Out Peak', 'VI SDK UUID'],
        'r' : []
    },

    'dvSwitch':{
        'f' : ['# VMs', 'Contact', 'CDP Operation', 'In Avg', 'In Traffic Shaping', 'Name', 'Admin Name', 'LACP Mode', 'Created', 'Max MTU', 'VI SDK Server', 'Out Traffic Shaping', 'In Peak', 'Out Avg', '# Ports', 'LACP Load Balance Alg.', 'Switch', 'Host members', 'In Burst', 'Vendor', 'LACP Name', 'CDP Type', 'Max Ports', 'Description', 'Datacenter', 'Version', 'Out Burst', 'Out Peak', 'VI SDK UUID'],
        'r' : []
    },

    'vCluster' : {
        'f':['Name', 'Config status', 'OverallStatus', 'NumHosts', 'numEffectiveHosts', 'TotalCpu', 'NumCpuCores', 'NumCpuThreads', 'Effective Cpu', 'TotalMemory', 'Effective Memory', 'Num VMotions', 'HA enabled', 'Failover Level', 'AdmissionControlEnabled', 'Host monitoring', 'HB Datastore Candidate Policy', 'Isolation Response', 'Restart Priority', 'Cluster Settings', 'Max Failures', 'Max Failure Window', 'Failure Interval', 'Min Up Time', 'VM Monitoring', 'DRS enabled', 'DRS default VM behavior', 'DRS vmotion rate', 'DPM enabled', 'DPM default behavior', 'DPM Host Power Action Rate', 'VI SDK Server', 'VI SDK UUID'],
        'r': []
    }
}


now = datetime.datetime.now()

for root, dir_, files in os.walk(filedir):
    if root == filedir:
        for file in files:
            workbook = os.path.join(root, file)
            workbook = xlrd.open_workbook(workbook)
            for sheet_name in sheets:
                sheet = workbook.sheet_by_name(sheet_name)
                sheet_fields = sheets[sheet_name]['f']

                rows = []
                for rindex in range(sheet.nrows):
                    row = []
                    for cindex in range(sheet.ncols):
                        row.append(sheet.cell(rindex, cindex).value)
                    rows.append(row)

                head = rows.pop(0)

                for row in rows:
                    row_ordered = []
                    for f in sheet_fields:
                        row_ordered.append(row[head.index(f)])

                    sheet_rows = sheets[sheet_name]['r']
                    sheet_rows.append(row_ordered)


with pymysql.connect(user='root', db='test') as cursor:
    for sheet_name in sheets:
        print(sheet_name)
        sheet_fields = sheets[sheet_name]['f']
        sheet_rows = sheets[sheet_name]['r']

        insert_fields = ','.join(['`{}`'.format(f) for f in sheet_fields ])
        insert_fields_values_placeholders = ','.join(['"%s"'] * len(sheet_fields))

        for i, row in enumerate(sheet_rows):
            sql = f'''INSERT INTO {sheet_name} ( {insert_fields} ) values '''

            try:
                sql += f'''( {insert_fields_values_placeholders} )''' % tuple(row)
                cursor.execute(sql)
            except Exception as e:
                pass
