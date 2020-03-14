import hashlib
import json
import logging
import os
import sys
from collections import defaultdict

import requests

from lib.authclient import KBaseAuth as _KBaseAuth
from lib.AbstractHandleClient import AbstractHandle as HandleService
from lib.AssemblySequenceAPIServiceClient import AssemblySequenceAPI
from lib.DataFileUtilClient import DataFileUtil
from lib.WSLargeDataIOClient import WsLargeDataIO
from lib.WorkspaceClient import Workspace as WorkspaceClient
from lib.GenomeFileUtil.core import GenomeUtils

MAX_GENOME_SIZE = 2**30


class GenomeInterface:
    def __init__(self, config):
        self.handle_url = config.handleURL
        self.shock_url = config.shockURL
        self.sw_url = config.srvWizURL
        self.token = config.token
        self.auth_service_url = config.authServiceUrl
        self.callback_url = config.callbackURL
        self.re_api_url = config.re_api_url
        self.auth_client = _KBaseAuth(self.auth_service_url)
        self.dfu = DataFileUtil(self.callback_url)
        self.taxon_wsname = config.raw['taxon-workspace-name']
        self.scratch = config.raw['scratch']
        self.ws_large_data = WsLargeDataIO(self.callback_url)
        self.ws = WorkspaceClient(url=config.ws, token=config.token)

    @staticmethod
    def _validate_save_one_genome_params(params):
        """
        _validate_save_one_genome_params:
                validates params passed to save_one_genome method
        """
        logging.info('start validating save_one_genome params')
        # check for required parameters
        for p in ['workspace', 'name', 'data']:
            if p not in params:
                raise ValueError(
                    '"{}" parameter is required, but missing'.format(p))

    def _check_shock_response(self, response, errtxt):
        """
        _check_shock_response: check shock node response (Copied from DataFileUtil)
        """
        logging.info('start checking shock response')

        if not response.ok:
            try:
                err = json.loads(response.content)['error'][0]
            except Exception:
                # this means shock is down or not responding.
                logging.error("Couldn't parse response error content from Shock: " + response.content)
                response.raise_for_status()
            raise ValueError(errtxt + str(err))

    def _own_handle(self, genome_data, handle_property):
        """
        _own_handle: check that handle_property point to shock nodes owned by calling user
        """

        logging.info('start checking handle {} ownership'.format(handle_property))

        if handle_property in genome_data:
            handle_id = genome_data[handle_property]
            hs = HandleService(self.handle_url, token=self.token)
            handles = hs.hids_to_handles([handle_id])
            shock_id = handles[0]['id']

            # Copy from DataFileUtil.own_shock_node implementation:
            header = {'Authorization': 'Oauth {}'.format(self.token)}
            res = requests.get(self.shock_url + '/node/' + shock_id +
                               '/acl/?verbosity=full',
                               headers=header, allow_redirects=True)
            self._check_shock_response(
                res, 'Error getting ACLs for Shock node {}: '.format(shock_id))
            owner = res.json()['data']['owner']['username']
            user_id = self.auth_client.get_user(self.token)

            if owner != user_id:
                logging.info('start copying node to owner: {}'.format(user_id))
                dfu_shock = self.dfu.copy_shock_node({'shock_id': shock_id,
                                                      'make_handle': True})
                handle_id = dfu_shock['handle']['hid']
                genome_data[handle_property] = handle_id

    def _check_dna_sequence_in_features(self, genome):
        """
        _check_dna_sequence_in_features: check dna sequence in each feature
        """
        logging.info('start checking dna sequence in each feature')

        if 'features' in genome:
            features_to_work = {}
            for feature in genome['features']:
                if not ('dna_sequence' in feature and feature['dna_sequence']):
                    features_to_work[feature['id']] = feature['location']

            if len(features_to_work) > 0:
                aseq = AssemblySequenceAPI(self.sw_url, token=self.token)
                get_dna_params = {'requested_features': features_to_work}
                if 'assembly_ref' in genome:
                    get_dna_params['assembly_ref'] = genome['assembly_ref']
                elif 'contigset_ref' in genome:
                    get_dna_params['contigset_ref'] = genome['contigset_ref']
                else:
                    # Nothing to do (it may be test genome without contigs)...
                    return
                dna_sequences = aseq.get_dna_sequences(get_dna_params)[
                    'dna_sequences']
                for feature in genome['features']:
                    if feature['id'] in dna_sequences:
                        feature['dna_sequence'] = dna_sequences[feature['id']]
                        feature['dna_sequence_length'] = len(feature['dna_sequence'])

    def get_one_genome(self, params):
        """Fetch a genome using WSLargeDataIO and return it as a python dict"""
        logging.info('fetching genome object')

        #result = self.ws_large_data.get_objects(params)
        result = self.ws.get_objects2(params)
        #~ with open(os.path.join(self.scratch, 'output.json'), 'w') as outfile:
            #~ outfile.write(json.dumps(result, indent=4))
        res = result['data'][0]
        #data = json.load(open(res['data_json_file']))
        return res['data'], res['info']# data, res['info']
        # return self.dfu.get_objects(params)['data'][0]

    def save_one_genome(self, params):
        logging.info('start saving genome object')
        self._validate_save_one_genome_params(params)
        workspace = params['workspace']
        name = params['name']
        data = params['data']
        # XXX there is no `workspace_datatype` param in the spec
        ws_datatype = params.get('workspace_datatype', "KBaseGenomes.Genome")
        # XXX there is no `meta` param in the spec
        meta = params.get('meta', {})
        if "AnnotatedMetagenomeAssembly" in ws_datatype:
            if params.get('upgrade') or 'feature_counts' not in data:
                data = self._update_metagenome(data)
        else:
            if params.get('upgrade') or 'feature_counts' not in data:
                data = self._update_genome(data)

        # check all handles point to shock nodes owned by calling user
        self._own_handle(data, 'genbank_handle_ref')
        self._own_handle(data, 'gff_handle_ref')
        if "AnnotatedMetagenomeAssembly" not in ws_datatype:
            self._check_dna_sequence_in_features(data)
            data['warnings'] = self.validate_genome(data)

        # sort data
        data = GenomeUtils.sort_dict(data)
        # dump genome to scratch for upload
        data_path = os.path.join(self.scratch, name + ".json")
        json.dump(data, open(data_path, 'w'))
        if 'hidden' in params and str(params['hidden']).lower() in ('yes', 'true', 't', '1'):
            hidden = 1
        else:
            hidden = 0

        if isinstance(workspace, int) or workspace.isdigit():
            workspace_id = workspace
        else:
            workspace_id = self.dfu.ws_name_to_id(workspace)

        save_params = {'id': workspace_id,
                       'objects': [{'type': ws_datatype,
                                    'data_json_file': data_path,
                                    'name': name,
                                    'meta': meta,
                                    'hidden': hidden}]}
        dfu_oi = self.ws_large_data.save_objects(save_params)[0]
        returnVal = {'info': dfu_oi, 'warnings': data.get('warnings', [])}
        return returnVal

    @staticmethod
    def determine_tier(source):
        """
        Given a user provided source parameter, assign a source and genome tier
        """
        low_source = source.lower()
        if 'refseq' in low_source:
            if 'reference' in low_source:
                return "RefSeq", ['Reference', 'Representative',
                                  'ExternalDB']
            if 'representative' in low_source:
                return "RefSeq", ['Representative', 'ExternalDB']
            if 'user' in low_source:
                return "RefSeq", ['ExternalDB', 'User']
            return "RefSeq", ['ExternalDB']
        if 'phytozome' in low_source:
            if 'flagship' in source:
                return "Phytosome", ['Reference', 'Representative',
                                     'ExternalDB']
            return "Phytosome", ['Representative', 'ExternalDB']
        if 'ensembl' in low_source:
            if 'user' in low_source:
                return "Ensembl", ['ExternalDB', 'User']
            return "Ensembl", ['Representative', 'ExternalDB']
        return source, ['User']

    def _update_metagenome(self, genome):
        """Checks for missing required fields and fixes breaking changes"""
        if 'molecule_type' not in genome:
            genome['molecule_type'] = 'Unknown'

    def _update_genome(self, genome):
        """Checks for missing required fields and fixes breaking changes"""
        # do top level updates
        ontologies_present = defaultdict(dict)  # type: dict
        ontologies_present.update(genome.get('ontologies_present', {}))
        ontology_events = genome.get('ontology_events', [])
        # NOTE: 'genome_tiers' not in Metagenome spec
        if 'genome_tiers' not in genome:
            genome['source'], genome['genome_tiers'] = self.determine_tier(genome['source'])
        if 'molecule_type' not in genome:
            genome['molecule_type'] = 'Unknown'

        # If an NCBI taxonomy ID is provided, fetch additional data about the taxon
        # NOTE: Metagenome object does not have a 'taxon_assignments' field
        if 'taxon_assignments' in genome and genome['taxon_assignments'].get('ncbi'):
            tax_id = int(genome['taxon_assignments']['ncbi'])
            GenomeUtils.set_taxon_data(tax_id, self.re_api_url, genome)
        else:
            GenomeUtils.set_default_taxon_data(genome)

        if any([x not in genome for x in ('dna_size', 'md5', 'gc_content', 'num_contigs')]):
            if 'assembly_ref' in genome:
                assembly_data = self.dfu.get_objects(
                    {'object_refs': [genome['assembly_ref']],
                     'ignore_errors': 0})['data'][0]['data']
                genome["gc_content"] = assembly_data['gc_content']
                genome["dna_size"] = assembly_data['dna_size']
                genome["md5"] = assembly_data['md5']
                genome["num_contigs"] = assembly_data['num_contigs']

                if assembly_data.get('type'):
                    genome['genome_type'] = assembly_data['type']

            elif 'contigset_ref' in genome:
                contig_data = self.dfu.get_objects(
                    {'object_refs': [genome['contigset_ref']],
                     'included': ['contigs/[*]/length', 'md5'],
                     'ignore_errors': 0})['data'][0]['data']
                genome["gc_content"] = None
                genome["dna_size"] = sum((c['length'] for c in contig_data['contigs']))
                genome["md5"] = contig_data['md5']
                genome["num_contigs"] = len(contig_data['contigs'])

        # NOTE: metagenomes do not have the following fields
        if 'cdss' not in genome:
            genome['cdss'] = []
        if 'mrnas' not in genome:
            genome['mrnas'] = []
        if 'non_coding_features' not in genome:
            genome['non_coding_features'] = []

        # do feature level updates
        retained_features = []
        type_counts = defaultdict(int)
        for field in ('mrnas', 'cdss', 'features'):
            for i, feat in enumerate(genome.get(field, [])):
                if 'function' in feat and not isinstance(feat, list):
                    feat['functions'] = feat['function'].split('; ')
                    del feat['function']
                if 'aliases' in feat:
                    if not feat['aliases']:
                        del feat['aliases']
                    elif not isinstance(feat['aliases'][0], (list, tuple)):
                        feat['aliases'] = [['gene_synonym', x] for x in feat['aliases']]
                if 'type' in feat:
                    type_counts[feat['type']] += 1
                for ontology, terms in feat.get('ontology_terms', {}).items():
                    for term in terms.values():
                        if isinstance(term, list):
                            continue
                        ontologies_present[ontology][term['id']] = term['term_name']
                        term_evidence = []
                        for ev in term['evidence']:
                            ev['id'] = ontology
                            ev['ontology_ref'] = term["ontology_ref"]
                            if ev not in ontology_events:
                                ontology_events.append(ev)
                            term_evidence.append(ontology_events.index(ev))
                        feat['ontology_terms'][ontology][term['id']] = term_evidence

                # remove deprecated fields
                feat.pop('protein_families', None)
                feat.pop('atomic_regulons', None)
                feat.pop('orthologs', None)
                feat.pop('coexpressed_fids', None)
                feat.pop('publications', None)
                feat.pop('regulon_data', None)
                feat.pop('subsystem_data', None)

                if 'dna_sequence_length' not in feat:
                    feat['dna_sequence_length'] = sum(x[3] for x in feat['location'])

                if 'protein_translation' in feat and 'protein_md5' not in feat:
                    feat['protein_md5'] = hashlib.md5(
                        feat.get('protein_translation', '').encode('utf8')
                    ).hexdigest()

                # split all the stuff lumped together in old versions into the
                # right arrays
                if field == 'features':
                    if feat.get('type', 'gene') == 'gene':
                        if not feat.get('cdss', []):
                            type_counts['non_coding_genes'] += 1
                            genome['non_coding_features'].append(feat)
                        else:
                            retained_features.append(feat)
                    elif feat.get('type', 'gene') == 'CDS':
                        if 'parent_gene' not in feat:
                            feat['parent_gene'] = ''
                        genome['cdss'].append(feat)
                    elif feat.get('type', 'gene') == 'mRNA':
                        if 'parent_gene' not in feat:
                            feat['parent_gene'] = ''
                        genome['mrnas'].append(feat)

        genome['features'] = retained_features
        if ontology_events:
            genome['ontology_events'] = ontology_events
        if ontologies_present:
            genome['ontologies_present'] = ontologies_present

        type_counts['mRNA'] = len(genome.get('mrnas', []))
        type_counts['CDS'] = len(genome.get('cdss', []))
        type_counts['protein_encoding_gene'] = len(genome['features'])
        type_counts['non_coding_features'] = len(
            genome.get('non_coding_features', []))
        genome['feature_counts'] = type_counts
        return genome

    @staticmethod
    def validate_genome(g):
        """
        Run a series of checks on the genome object and return any warnings
        """

        allowed_tiers = {'Representative', 'Reference', 'ExternalDB', 'User'}

        logging.info('Validating genome object contents')
        warnings = g.get('warnings', [])

        # TODO: Determine whether these checks make any sense for Metagenome
        #       object. Looks like many don't.
        #       Add validations for Metagenome object

        # this will fire for some annotation methods like PROKKA
        if g.get('domain') == "Bacteria" and len(g.get('cdss', [])) != len(g['features']):
            warnings.append("For prokaryotes, CDS array should generally be the"
                            " same length as the Features array.")

        if g.get('domain') == "Eukaryota" and len(g.get('features', [])) == len(g.get('cdss', [])):
            warnings.append("For Eukaryotes, CDS array should not be the same "
                            "length as the Features array due to RNA splicing.")

        if g.get('molecule_type') not in {"DNA", 'ds-DNA'}:
            if g.get('domain', '') not in {'Virus', 'Viroid'} and \
                            g['molecule_type'] not in {"DNA", 'ds-DNA'}:
                warnings.append("Genome molecule_type {} is not expected "
                                "for domain {}.".format(g['molecule_type'],
                                                        g.get('domain', '')))

        if "genome_tiers" in g and set(g['genome_tiers']) - allowed_tiers:
            warnings.append("Undefined terms in genome_tiers: " + ", ".join(
                set(g['genome_tiers']) - allowed_tiers))
        assignments = g.get('taxon_assignments', {})
        if 'ncbi' not in assignments or (
                'taxon_ref' in g and g['taxon_ref'] == "ReferenceTaxons/unknown_taxon"):
            warnings.append('Unable to determine organism taxonomy')

        GenomeInterface.handle_large_genomes(g)
        return warnings

    @staticmethod
    def handle_large_genomes(g):
        """Determines the size of various feature arrays and starts removing the dna_sequence if
        the genome is getting too big to store in the workspace"""
        def _get_size(obj):
            return sys.getsizeof(json.dumps(obj))

        # seems pretty uneccessary...
        def sizeof_fmt(num):
            for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
                if abs(num) < 1024.0:
                    return "%3.1f %sB" % (num, unit)
                num /= 1024.0
            return "%.1f %sB" % (num, 'Yi')

        feature_lists = ('mrnas', 'features', 'non_coding_features', 'cdss')
        master_key_sizes = dict()
        # Change want full breakdown to True if want to see break down of sizes.
        # By making this a changeable flag it will run faster for standard uploads.
        want_full_breakdown = False
        for x in feature_lists:
            if x in g:
                need_to_remove_dna_sequence = _get_size(g) > MAX_GENOME_SIZE
                if need_to_remove_dna_sequence or want_full_breakdown:
                    feature_type_dict_keys = dict()
                    for feature in g[x]:
                        for feature_key in list(feature.keys()):
                            if feature_key == "dna_sequence" and need_to_remove_dna_sequence:
                                # NOTE: should this get stored somewhere?
                                del (feature["dna_sequence"])
                            else:
                                if feature_key not in feature_type_dict_keys:
                                    feature_type_dict_keys[feature_key] = 0
                                feature_type_dict_keys[feature_key] += sys.getsizeof(
                                    feature[feature_key])
                    for feature_key in feature_type_dict_keys:
                        feature_type_dict_keys[feature_key] = sizeof_fmt(
                            feature_type_dict_keys[feature_key])
                    master_key_sizes[x] = feature_type_dict_keys
                print(f"{x}: {sizeof_fmt(_get_size(g[x]))}")
        total_size = _get_size(g)
        print(f"Total size {sizeof_fmt(total_size)} ")
        if want_full_breakdown:
            print(f"Here is the breakdown of the sizes of feature lists elements : "
                  f"{str(master_key_sizes)}")
        if total_size > MAX_GENOME_SIZE:
            print(f"Here is the breakdown of the sizes of feature lists elements : "
                  f"{str(master_key_sizes)}")
            raise ValueError(f"This genome size of {sizeof_fmt(total_size)} exceeds the maximum "
                             f"permitted size of {sizeof_fmt(MAX_GENOME_SIZE)}.\n"
                             f"Here is the breakdown for feature lists and their respective "
                             f"sizes:\n{master_key_sizes}")
