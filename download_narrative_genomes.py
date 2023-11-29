#!/usr/bin/env python
import os
import sys
import argparse
import json as _json
import requests as _requests
from configparser import ConfigParser
from pathlib import Path

from lib.authclient import KBaseAuth as _KBaseAuth
from lib.GenomeFileUtil.core.GenomeToGFF import GenomeToGFF
from lib.GenomeFileUtil.core.GenomeToGenbank import GenomeToGenbank
from lib.GenomeFileUtil.core.GenomeFeaturesToFasta import GenomeFeaturesToFasta
from lib.AssemblyUtilClient import AssemblyUtil
from lib.WorkspaceClient import Workspace
from lib.DataFileUtilClient import DataFileUtil

class MethodContext(dict):

    def __init__(self, logger):
        self['client_ip'] = None
        self['user_id'] = None
        self['authenticated'] = None
        self['token'] = None
        self['module'] = None
        self['method'] = None
        self['call_id'] = None
        self['rpc_context'] = None
        self['provenance'] = None

    def provenance(self):
        return self.get('provenance')


def get_args():
    """Returns command-line arguments"""
    desc = '''This program downloads genome objects from a KBase narrative.'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-n', dest='narrative', type=str, help='Narrative identiier (for example, 49058 from https://narrative.kbase.us/narrative/49058)')
    parser.add_argument('-t', dest='token', type=str, help='Authorization token from kbase_session or kbase_session_backup field of kbase.us cookies')
    parser.add_argument('-f', dest='format', type=str, help='File format. Acceptable values: gbk, gff, faa. Export to GenBank also generates nuclotide fasta files in \"contigs\" subdirectory')
    args = parser.parse_args()
    acceptable_formats = ('gbk', 'gff', 'faa')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    elif args.format not in acceptable_formats:
        print('Format not supported:', args.format)
        sys.exit(1)
    return (args.token, args.narrative, args.format)


def check_token(auth_svc, token):
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': token}
    ret = _requests.get(auth_svc, headers=headers, allow_redirects=True)
    #print(ret.request.headers)
    #print(ret.request)
    #print(ret.headers)
    #print('ret', ret.text)

    status = ret.status_code
    if status >= 200 and status <= 299:
        print('TOKEN VALIDATED')
        tok = _json.loads(ret.text)
    elif status == 401:
        raise Exception('INVALID TOKEN: ' + ret.text)
    else:
        raise Exception(ret.text)
    return tok['user']


def list_objects(token, my_ws):

    ws_client = Workspace(url='https://kbase.us/services/ws', token=token)
    ret = ws_client.get_workspace_info({'id':my_ws})
    print('WORKSPACE VALIDATED:', ret)
    
    my_workspace = ret[1]
    max_id = ret[4]
    kb_data_type = 'KBaseGenomes.Genome'
    
    out_file = my_ws + '_' + kb_data_type + '_list.txt'
    with open(out_file, 'w') as outfile:
        for i in range(0, max_id, 10000):
            obj_list = ws_client.list_objects({
                'workspaces':[my_workspace],
                'minObjectID': i + 1,
                'maxObjectID': i + 10000}
            )
            #print(obj_list)
            for obj in obj_list:
                if kb_data_type in str(obj[2]):
                    outfile.write('\t'.join([str(x) for x in obj]) + '\n')
    return out_file


def kb_genomes_download(genomes_list, cfg):

    genomes = []
    with open(genomes_list, 'r') as f:
        for line in f:
            row = line.rstrip('\n\r').split('\t')
            genomes.append('/'.join([row[6], row[0], row[4]]))
    print(str(len(genomes)), 'genomes found')
    
    config = ConfigParser()
    config.callbackURL = cfg['njsw-url']
    config.sharedFolder = cfg['scratch']
    config.handleURL = cfg['handle-service-url']
    config.shockURL = cfg['shock-url']
    config.srvWizURL = cfg['srv-wiz-url']
    config.token = cfg['context']['token']
    config.authServiceUrl = cfg['auth-service-url']
    config.authServiceUrl = cfg['auth-service-url']
    config.ws = cfg['workspace-url']
    config.re_api_url = cfg['re-api-url']
    config.raw = cfg
    config.raw['taxon-workspace-name'] = cfg['workspace-id']
    
    download_format = cfg['download_format']
    if download_format == 'gbk':
        target_dir = 'genbank'
        exporter = GenomeToGenbank(config)
    elif download_format == 'gff':
        target_dir = 'gff'
        exporter = GenomeToGFF(config)
    elif download_format == 'faa':
        target_dir = 'proteins'
        exporter = GenomeFeaturesToFasta(config)
    Path(target_dir).mkdir(exist_ok=True)

    for genome_ref in genomes:
        download_params = {'genome_ref': genome_ref, 'target_dir': target_dir}
        if download_format == 'gbk':
            result = exporter.export(cfg['context'], download_params)['genbank_file']['file_path']
        elif download_format == 'faa':
            result = exporter.export(cfg['context'], download_params, protein=True)['file_path']
        elif download_format == 'gff':
            result = exporter.export(cfg['context'], download_params)['file_path']
        print('FILE CREATED:', result)


def main():
    token, my_ws_id, file_format = get_args()

    # Getting username from Auth profile for token
    auth_svc = 'https://kbase.us/services/auth/api/V2/token'
    user_id = check_token(auth_svc, token)

    context = {'token': token,
               'user_id': user_id,
               'provenance': [
                {'service': 'KBaseSwiper',
                 'method': 'please_never_use_it_in_production',
                 'method_params': []
                 }],
               'authenticated': 1}

    cfg = {'kbase-endpoint': 'https://kbase.us/services', 
            'job-service-url': 'https://kbase.us/services/userandjobstate', 
            'workspace-url': 'https://kbase.us/services/ws', 
            'shock-url': 'https://kbase.us/services/shock-api', 
            'handle-service-url': 'https://kbase.us/services/handle_service', 
            'srv-wiz-url': 'https://kbase.us/services/service_wizard', 
            'njsw-url': 'https://kbase.us/services/njs_wrapper', 
            'auth-service-url': 'https://kbase.us/services/auth/api/legacy/KBase/Sessions/Login', 
            're-api-url': 'https://kbase.us/services/relation_engine_api',
            'auth-service-url-allow-insecure': 'false', 
            'scratch': os.getcwd(),
            'workspace-id': my_ws_id,
            'context': context,
            'download_format': file_format,
            'pigz_n_processes': 1,
            'pigz_compression_level': 1}

 
    genomes_file = list_objects(token, my_ws_id)
    kb_genomes_download(genomes_file, cfg)

    # Remove temp directory created by DataFileUtil
    if os.path.exists(os.path.join(cfg['scratch'],'temp')):
        os.rmdir(os.path.join(cfg['scratch'],'temp'))

if __name__=='__main__':
    main()
