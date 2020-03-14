"""
Genome CDS Protein Sequence to Fasta file conversion.
"""
import logging
import os
import textwrap

from lib.GenomeFileUtil.core.GenomeInterface import GenomeInterface
from lib.DataFileUtilClient import DataFileUtil

class GenomeFeaturesToFasta(object):
    def __init__(self, sdk_config):
        self.cfg = sdk_config
        self.dfu = DataFileUtil(self.cfg.callbackURL)
        self.gi = GenomeInterface(sdk_config)
        self.default_params = {
            'genome_ref': None,
            'feature_lists': ['features'],
            'filter_ids': [],
            'include_functions': True,
            'include_aliases': True,
            'target_dir': 'fasta'
        }
        self.valid_feature_lists = {'features', 'mrnas', 'cdss', 'non_coding_features'}

    def validate_params(self, params):
        if 'genome_ref' not in params:
            raise ValueError('required field "genome_ref" was not defined')

        unknown_feature_lists = set(params.get('feature_lists', [])) - self.valid_feature_lists
        if unknown_feature_lists:
            raise ValueError(f"Unknown feature_lists specified: {unknown_feature_lists}. "
                             f"Must be one of {self.valid_feature_lists}")

        unknown_params = set(params) - set(self.default_params)
        if unknown_params:
            raise ValueError(f"Unknown parameter(s) specified: {unknown_params}")
        
    def export(self, ctx, user_params, protein=False):
        # 1) validate parameters and extract defaults
        self.validate_params(user_params)
        params = self.default_params.copy()
        params.update(user_params)
        params['filter_ids'] = set(params['filter_ids'])

        # 2) get genome info
        #~ genome_data = self.dfu.get_objects({
            #~ 'object_refs': [params['genome_ref']]
        #~ })['data'][0]
        
        #~ info = genome_data['info']
        #~ data = genome_data['data']
        data, info = self.gi.get_one_genome({'objects': [{"ref": params['genome_ref']}]})
        
        # 3) make sure the type is valid
        if info[2].split(".")[1].split('-')[0] != 'Genome':
            raise ValueError('Object is not a Genome, it is a:' + str(info[2]))
        if 'feature_counts' not in data:
            logging.warning("Updating legacy genome")
            data = self.gi._update_genome(data)

        # 4) build the fasta file and return it
        if protein:
            file_path = self._build_fasta_file(data.get('cdss'), info[1] + '_protein.faa',
                                               'protein_translation', params)
        else:
            feature_gen = (feat for feat_list in params['feature_lists']
                                for feat in data.get(feat_list))
            file_path = self._build_fasta_file(feature_gen, info[1] + '_features.fna',
                                               'dna_sequence', params)

        return {'file_path': file_path}

    def _build_fasta_file(self, features, output_filename, seq_key, params):
        if 'target_dir' in params:
            file_path = os.path.join(self.cfg.sharedFolder, params['target_dir'], output_filename)
        else:
            file_path = os.path.join(self.cfg.sharedFolder, output_filename)
        logging.info(f"Saving FASTA to {file_path}")
        missing_seq = 0
        with open(file_path, "w") as out_file:
            for feat in features:
                if params['filter_ids'] and feat['id'] not in params['filter_ids']:
                    continue
                if not feat.get(seq_key):
                    missing_seq += 1
                    continue

                header_line = self._build_header(feat,
                                                 params['include_functions'],
                                                 params['include_aliases'])
                out_file.write(header_line+"\n")
                out_file.write(textwrap.fill(feat.get(seq_key))+"\n")

        if missing_seq:
            logging.warning(
                f"{missing_seq} items were missing a {seq_key} attribute and were skipped")
        return file_path

    @staticmethod
    def _build_header(feat, include_functions, include_aliases):
        header_line = ">{}".format(feat["id"])

        if include_functions:
            if feat.get("functions"):
                header_line += f' functions={",".join(feat["functions"])}'
            if feat.get("functional_descriptions"):
                header_line += f' functional_descriptions={",".join(feat["functional_descriptions"])}'

        if include_aliases:
            if feat.get("aliases"):
                alias = (alias[1] for alias in feat["aliases"])
                header_line += f" aliases={','.join(alias)}"
            if feat.get("db_xrefs"):
                db_xref_info = (f"{db_xref[0]}:{db_xref[1]}" for db_xref in feat["db_xrefs"])
                header_line += f" db_xrefs={','.join(db_xref_info)}"

        return header_line
