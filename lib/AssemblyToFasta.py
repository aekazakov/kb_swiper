import os
import shutil

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
#from Bio.Alphabet import SingleLetterAlphabet

from lib.DataFileUtilClient import DataFileUtil


class AssemblyToFasta:

    def __init__(self, callback_url, scratch):
        self.scratch = scratch
        self.dfu = DataFileUtil(callback_url)


    def export_as_fasta(self, ctx, params):
        """ Used almost exclusively for download only """
        # validate parameters
        if 'input_ref' not in params:
            raise ValueError('Cannot export Assembly- not input_ref field defined.')

        # export to a file
        file = self.assembly_as_fasta(ctx, {'ref': params['input_ref']})

        # create the output directory and move the file there
        export_package_dir = os.path.join(self.scratch, file['assembly_name'])
        os.makedirs(export_package_dir)
        shutil.move(file['path'], os.path.join(export_package_dir, os.path.basename(file['path'])))

        # package it up and be done
        package_details = self.dfu.package_for_download({'file_path': export_package_dir,
                                                         'ws_refs': [params['input_ref']]
                                                         })

        return {'shock_id': package_details['shock_id']}



    def assembly_as_fasta(self, params):
        """ main function that accepts a ref to an object and writes a file """

        self.validate_params(params)

        print(f'downloading ws object data ({ params["ref"]})')
        assembly_object = self.dfu.get_objects({'object_refs': [params['ref']]})['data'][0]
        ws_type = assembly_object['info'][2]
        obj_name = assembly_object['info'][1]

        if 'filename' in params:
            output_filename = params['filename']
        else:
            output_filename = obj_name + '.fa'

        output_fasta_file_path = os.path.join(self.scratch, output_filename)

        if 'KBaseGenomes.ContigSet' in ws_type:
            self.process_legacy_contigset(output_fasta_file_path,
                                          assembly_object['data'])
        elif 'KBaseGenomeAnnotations.Assembly' in ws_type:
            self.process_assembly(output_fasta_file_path, assembly_object['data'])

        else:
            raise ValueError('Cannot write data to fasta; invalid WS type (' + ws_type +
                             ').  Supported types are KBaseGenomes.ContigSet and ' +
                             'KBaseGenomeAnnotations.Assembly')

        return {'path': output_fasta_file_path, 'assembly_name': obj_name}

    def fasta_rows_generator_from_contigset(self, contig_list):
        """ generates SeqRecords iterator for writing from a legacy contigset object """
        for contig in contig_list:
                description = ''
                if 'description' in contig and contig['description']:
                    description = contig['description']
<<<<<<< HEAD
                yield SeqRecord(Seq(contig['sequence']),  #, SingleLetterAlphabet),
=======
                yield SeqRecord(Seq(contig['sequence']),
>>>>>>> refs/remotes/origin/master
                                id=contig['id'],
                                description=description)

    def process_legacy_contigset(self, output_fasta_path, data):
        SeqIO.write(self.fasta_rows_generator_from_contigset(data['contigs']),
                    output_fasta_path,
                    "fasta")

    def process_assembly(self, output_fasta_path, data):
        self.dfu.shock_to_file({'handle_id': data['fasta_handle_ref'],
                                'file_path': output_fasta_path,
                                'unpack': 'uncompress'
                                })

    def validate_params(self, params):
        for key in ['ref']:
            if key not in params:
                raise ValueError('required "' + key + '" field was not defined')
