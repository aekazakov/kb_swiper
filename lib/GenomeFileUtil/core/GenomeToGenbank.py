"""
GenomeAnnotation to GenBank file conversion.
"""

import os
import logging
import time
# import json
from collections import defaultdict

from Bio import SeqIO, SeqFeature, Alphabet

from lib.AssemblyUtilClient import AssemblyUtil
from lib.DataFileUtilClient import DataFileUtil
from lib.WorkspaceClient import Workspace as WorkspaceClient
from lib.baseclient import ServerError
from lib.GenomeFileUtil.core.GenomeInterface import GenomeInterface
from lib.AssemblyToFasta import AssemblyToFasta
from lib.DataFileUtilImpl import DataFileUtil as DataFileUtilImpl
STD_PREFIX = " " * 21
CONTIG_ID_FIELD_LENGTH = 16


class GenomeToGenbank(object):

    def __init__(self, sdk_config):
        self.cfg = sdk_config
        self.dfu = DataFileUtil(self.cfg.callbackURL)
        self.gi = GenomeInterface(sdk_config)

    def validate_params(self, params):
        if 'genome_ref' not in params:
            raise ValueError('required "genome_ref" field was not defined')

    def export(self, ctx, params):
        # 1) validate parameters and extract defaults
        self.validate_params(params)

        # 2) get genome info
        data, info = self.gi.get_one_genome({'objects': [{"ref": params['genome_ref']}]})

        # 3) make sure the type is valid
        if info[2].split(".")[1].split('-')[0] != 'Genome':
            raise ValueError('Object is not a Genome, it is a:' + str(info[2]))

        # 4) build the genbank file and return it
        logging.info('not cached, building file...')
        outfile = info[1] + ".gbff"
        if 'target_dir' in params:
            outfile = os.path.join(params['target_dir'], outfile)
        result = self.build_genbank_file(data, outfile,
                                         params['genome_ref'])
        if result is None:
            raise ValueError('Unable to generate file.  Something went wrong')
        result['from_cache'] = 0
        return result

    def export_original_genbank(self, ctx, params):
        # 1) validate parameters and extract defaults
        self.validate_params(params)

        # 2) get genome genbank handle reference
        data, info = self.gi.get_one_genome({'objects': [{"ref": params['genome_ref']}]})

        # 3) make sure the type is valid
        if info[2].split(".")[1].split('-')[0] != 'Genome':
            raise ValueError('Object is not a Genome, it is a:' + str(info[2]))

        # 4) if the genbank handle is there, get it and return
        logging.info('checking if genbank file is cached...')
        result = self.get_genbank_handle(data)
        return result

    def get_genbank_handle(self, data):
        if 'genbank_handle_ref' not in data:
            return None
        if data['genbank_handle_ref'] is None:
            return None

        logging.info(f"pulling cached genbank file from Shock: {data['genbank_handle_ref']}")
        file = self.dfu.shock_to_file({
                            'handle_id': data['genbank_handle_ref'],
                            'file_path': self.cfg.sharedFolder,
                            'unpack': 'unpack'
                        })
        return {
            'genbank_file': {
                'file_path': file['file_path']
            }
        }

    def build_genbank_file(self, genome_data, output_filename, genome_ref):
        g = GenomeFile(self.cfg, genome_data, genome_ref)
        file_path = os.path.join(self.cfg.sharedFolder, output_filename)
        g.write_genbank_file(file_path)

        return {
            'genbank_file': {
                'file_path': file_path
            }
        }


class GenomeFile:
    def __init__(self, cfg, genome_object, genome_ref):
        self.cfg = cfg
        self.genome_object = genome_object
        self.genome_ref = genome_ref
        self.seq_records = []
        self.features_by_contig = defaultdict(list)
        self.renamed_contigs = 0
        self.child_dict = {}
        """There is two ways of printing, if a feature has a parent_gene, it 
                will be printed breadth first when it's parent parent gene is printed.
                if not, it needs to be added to the features_by_contig to be printed"""
        # sort every feature in the feat_arrays into a dict by contig
        for feature in genome_object['features'] + genome_object.get(
                'non_coding_features', []):
            # type is not present in new gene array
            if 'type' not in feature:
                feature['type'] = 'gene'
            self.features_by_contig[feature['location'][0][0]].append(feature)

        for mrna in genome_object.get('mrnas', []):
            mrna['type'] = 'mRNA'
            if mrna.get('parent_gene'):
                self.child_dict[mrna['id']] = mrna
            else:
                self.features_by_contig[mrna['location'][0][0]].append(mrna)

        for cds in genome_object.get('cdss', []):
            cds['type'] = 'CDS'
            if cds.get('parent_gene'):
                self.child_dict[cds['id']] = cds
            else:
                self.features_by_contig[cds['location'][0][0]].append(cds)

        assembly_file_path, circ_contigs = self._get_assembly(genome_object)
        for contig in SeqIO.parse(open(assembly_file_path), 'fasta', Alphabet.generic_dna):
            if contig.id in circ_contigs:
                contig.annotations['topology'] = "circular"
            self._parse_contig(contig)

    def _get_assembly(self, genome):
        if 'assembly_ref' in genome:
            assembly_ref = genome['assembly_ref']
        else:
            assembly_ref = genome['contigset_ref']
        logging.info('Assembly reference = ' + assembly_ref)
        logging.info('Downloading assembly')
        #dfu = DataFileUtil(self.cfg.callbackURL)
        ws = WorkspaceClient(url=self.cfg.ws, token=self.cfg.token)
        
        logging.info(f'object_refs:{self.genome_ref};{assembly_ref}')

        #~ assembly_data = dfu.get_objects({
            #~ 'object_refs': [f'{self.genome_ref};{assembly_ref}']
        #~ })['data'][0]['data']
        res = ws.get_objects2({'objects': [{"ref": assembly_ref}]})['data'][0]
        assembly_data = res['data']
        assembly_info = res['info']
        #~ print(assembly_info)
        #~ with open(os.path.join(self.cfg.sharedFolder, 'temp.json'), 'w') as outfile:
            #~ outfile.write(json.dumps(assembly_data, indent=1))

        a2f = AssemblyToFasta(self.cfg.callbackURL, self.cfg.sharedFolder)
        assembly_file_path = os.path.join(self.cfg.sharedFolder, 'contigs', assembly_info[1] + '.fa')
        if isinstance(assembly_data['contigs'], dict):  # is an assembly
            circular_contigs = set([x['contig_id'] for x in list(assembly_data['contigs'].values())
                                    if x.get('is_circ')])
            print('Calling shock_to_file for node', assembly_data['fasta_handle_info']['shock_id'], assembly_file_path)
            dfui = DataFileUtilImpl(self.cfg.raw)
            result = dfui.shock_to_file({'token': self.cfg.token}, {'shock_id': assembly_data['fasta_handle_info']['shock_id'],
                        'file_path': assembly_file_path,
                        'unpack': 'uncompress'
                        })
            print('shock_to_file returns:', result)
        else:  # is a contig set
            circular_contigs = set([x['id'] for x in assembly_data['contigs']
                                    if x.get('replicon_geometry') == 'circular'])
            SeqIO.write(a2f.fasta_rows_generator_from_contigset(assembly_data['contigs']),
                        assembly_file_path,
                        "fasta")

        #~ au = AssemblyUtil(self.cfg.callbackURL)
        #~ assembly_file_path = au.get_assembly_as_fasta(
            #~ {'ref': f'{self.genome_ref};{assembly_ref}'}
        #~ )['path']
        return assembly_file_path, circular_contigs

    def _parse_contig(self, raw_contig):
        def feature_sort(feat):
            order = ('gene', 'mRNA', 'CDS')
            if feat['type'] not in order:
                priority = len(order)
            else:
                priority = order.index(feat['type'])
            start = min(x[1] for x in feat['location'])
            return start, priority

        go = self.genome_object  # I'm lazy
        raw_contig.dbxrefs = self.genome_object.get('aliases', [])
        taxonomy = [tax.strip() for tax in go.get('taxonomy', '').split(';')]
        raw_contig.annotations.update({
            "comment": go.get('notes', ""),
            "source": "KBase_" + go.get('source', ""),
            "taxonomy": taxonomy,
            "organism": go.get('scientific_name', ""),
            "date": time.strftime("%d-%b-%Y",
                                  time.localtime(time.time())).upper()
        })
        if not self.seq_records:  # Only on the first contig
            raw_contig.annotations['references'] = self._format_publications()
            logging.info("Added {} references".format(
                len(raw_contig.annotations['references'])))
            if 'notes' in go:
                raw_contig.annotations['comment'] = go['notes']

        if len(raw_contig.name) > CONTIG_ID_FIELD_LENGTH:
            raw_contig.annotations['comment'] = raw_contig.annotations.get('comment', "") + (
                f"Renamed contig from {raw_contig.name} because the original name exceeded "
                f"{CONTIG_ID_FIELD_LENGTH} characters"
            )
            self.renamed_contigs += 1
            raw_contig.name = f"scaffold{self.renamed_contigs:0>8}"

        if raw_contig.id in self.features_by_contig:
            # sort all features except for cdss and mrnas
            self.features_by_contig[raw_contig.id].sort(key=feature_sort)
            for feat in self.features_by_contig[raw_contig.id]:
                raw_contig.features.append(self._format_feature(feat, raw_contig.id))
                # process child mrnas & cdss if present
                raw_contig.features.extend([self._format_feature(
                    self.child_dict[_id], raw_contig.id) for _id in feat.get('mrnas', [])])
                raw_contig.features.extend([self._format_feature(
                    self.child_dict[_id], raw_contig.id) for _id in feat.get('cdss', [])])


        self.seq_records.append(raw_contig)

    def _format_publications(self):
        references = []
        for pub in self.genome_object.get('publications', []):
            if len(pub) != 7:
                logging.warning(f'Skipping unparseable publication {pub}')
            ref = SeqFeature.Reference()
            if pub[0]:
                ref.pubmed_id = str(pub[0])
            ref.title = pub[2]
            ref.authors = pub[5]
            ref.journal = pub[6]
            references.append(ref)
        return references

    def _format_feature(self, in_feature, current_contig_id):
        def _trans_loc(loc):
            # Don't write the contig ID in the loc line unless it's trans-spliced
            if loc[0] == current_contig_id:
                loc[0] = None
            if loc[2] == "-":
                return SeqFeature.FeatureLocation(loc[1]-loc[3], loc[1], -1, loc[0])
            else:
                return SeqFeature.FeatureLocation(loc[1]-1, loc[1]+loc[3]-1, 1, loc[0])

        # we have to do it this way to correctly make a "CompoundLocation"
        location = _trans_loc(in_feature['location'].pop(0))
        while in_feature['location']:
            location += _trans_loc(in_feature['location'].pop(0))
        out_feature = SeqFeature.SeqFeature(location, in_feature['type'])
        
        # Added locus tags
        if in_feature['type'] == 'gene':
            out_feature.qualifiers['locus_tag'] = in_feature['id']
        elif in_feature['type'] == 'CDS' and 'parent_gene' in in_feature:
            out_feature.qualifiers['locus_tag'] = in_feature['parent_gene']
        elif in_feature['type'] == 'mRNA' and 'parent_gene' in in_feature:
            out_feature.qualifiers['locus_tag'] = in_feature['parent_gene']
        
        if in_feature.get('functional_descriptions'):
            out_feature.qualifiers['function'] = "; ".join(
                    in_feature['functional_descriptions'])
        if in_feature.get('functions'):
            out_feature.qualifiers['product'] = "; ".join(in_feature['functions'])
        if 'function' in in_feature:
            out_feature.qualifiers['product'] = in_feature['function']

        if in_feature.get('note', False):
            out_feature.qualifiers['note'] = in_feature['note']
        if in_feature.get('protein_translation', False):
            out_feature.qualifiers['translation'] = in_feature['protein_translation']
        if in_feature.get('db_xrefs', False):
            out_feature.qualifiers['db_xref'] = ["{}:{}".format(*x) for x in
                                                 in_feature['db_xrefs']]
        if in_feature.get('ontology_terms', False):
            if 'db_xref' not in out_feature.qualifiers:
                out_feature.qualifiers['db_xref'] = []
            for ont, terms in in_feature['ontology_terms'].items():
                out_feature.qualifiers['db_xref'].extend([t for t in terms])

        for alias in in_feature.get('aliases', []):
            if len(alias) == 2:
                if not alias[0] in out_feature.qualifiers:
                    out_feature.qualifiers[alias[0]] = []
                out_feature.qualifiers[alias[0]].append(alias[1])
            else:  # back compatibility
                if 'db_xref' not in out_feature.qualifiers:
                    out_feature.qualifiers['db_xref'] = []
                out_feature.qualifiers['db_xref'].append(alias)

        for flag in in_feature.get('flags', []):
            out_feature.qualifiers[flag] = None

        if 'inference_data' in in_feature:
            out_feature.qualifiers['inference'] = [
                ":".join([x[y] for y in ('category', 'type', 'evidence') if x[y]])
                for x in in_feature['inference_data']]

        if in_feature.get('warnings', False):
            out_feature.qualifiers['note'] = out_feature.qualifiers.get(
                'note', "") + "Warnings: " + ",".join(in_feature['warnings'])

        return out_feature

    def write_genbank_file(self, file_path):
        if not self.seq_records:
            raise ValueError("No sequence data to write!")
        SeqIO.write(self.seq_records, open(file_path, 'w'), 'genbank')
