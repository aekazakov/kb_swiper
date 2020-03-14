import json
import logging
import os
import re
import time

from lib.relation_engine_client import REClient
from lib.relation_engine_client.exceptions import RENotFound

# Name of the ncbi taxonomy namespace stored in "taxon_assignments"
_NCBI_TAX = 'ncbi'

warnings = {
    "cds_excluded": "SUSPECT: CDS from {} was excluded because the associated "
                    "CDS failed coordinates validation",
    "cds_mrna_cds": "Feature order suggests that {} is the parent mRNA, but it"
                    " fails location validation",
    "cds_mrna_mrna": "Potential child CDS relationship failed due to location "
                    "validation.",
    "no_spoof": "Some CDS features in the file do not have a parent gene. "
                "Ensure the correct file source is selected, correct the source file "
                "or select the 'generate_missing_genes' option.",
    "spoofed_gene": "This gene was not in the source GenBank or GFF file. It was "
                    "added to be the parent of a CDS.",
    "spoofed_genome": "SUSPECT: This genome has {} genes that needed to be "
                      "spoofed for existing parentless CDS.",
    "not_trans_spliced": "The feature coordinates order are suspect and the "
                         "feature is not flagged as being trans-spliced",
    "genome_not_trans_spliced": "SUSPECT: This Genome has {} features with "
                                "coordinates that are out of order and are "
                                "not trans_splicing.",
    "inconsistent_CDS_length": "This CDS has a length of {} which is not "
                               "consistent with the length of the translation "
                               "included ({} amino acids).",
    "genome_inc_CDS_length": "SUSPECT: CDS {} has a length of {} which is "
                             "not consistent with the length of the "
                             "translation included ({} amino acids).",
    "inconsistent_translation": "The annotated protein translation is not "
                                "consistent with the recorded DNA sequence.",
    "genome_inc_translation": "SUSPECT: This Genome has a high proportion "
                              "({} out of {}) CDS features that do not "
                              "translate the supplied translation.",
    "no_translation_supplied": "This CDS did not have a supplied "
                               "translation. The translation is derived "
                               "directly from DNA sequence.",
    "coordinates_off_end": "SUSPECT: Feature {} has invalid coordinates off "
                           "of the end of the contig and was not included.",
    "non_exact_coordinates": "The coordinates supplied for this feature are "
                             "non-exact. DNA or protein translations are "
                             "approximate.",
    "not_multiple_of_3CDS": "Sequence length {} is not a multiple of three",
    "non_standard_start_codon": "First codon '{}' is not a start codon",
    "out_of_order": "The feature coordinates order are out of order. GFF typically does not "
                    "designate trans_splicing.",
    "both_strand_coordinates": "The feature coordinates are both strands. GFF typically does not "
                    "designate trans_splicing.",
    "premature_stop_codon": "Extra in frame stop codon found.",
    "mRNA_fail_parent_coordinate_validation": "This mRNA lists CDS {} as its "
                    "corresponding CDS, but it fails coordinate validation.",
    "CDS_fail_child_of_mRNA_coordinate_validation": "This CDS lists mRNA {} as its "
                    "corresponding mRNA, but it fails coordinate validation.",
    "CDS_fail_child_of_gene_coordinate_validation": "This CDS lists gene {} as its "
                    "corresponding gene, but it fails coordinate validation.",
    "genes_mRNA_child_fails_location_validation": "The mRNA {} lists this gene as its "
                    "corresponding parent gene, but it fails coordinate validation.",
    "genes_CDS_child_fails_location_validation": "The CDS {} lists this gene as a "
                    "corresponding parent gene, but it fails coordinate validation.",
    "mRNAs_parent_gene_fails_location_validation": "This mRNA lists gene {} as its "
                    "corresponding parent gene, but it fails coordinate validation.",
    "generic_parents_child_fails_location_validation": "This feature lists feature {} as its "
                    "corresponding child, but it fails coordinate validation.",
    "generic_childs_parent_fails_location_validation": "This feature lists feature {} as its "
                    "corresponding parent, but it fails coordinate validation.",
    "gff_odd_strand_type": "This feature had \"{}\" as the strand designation and not + or -. "
                    "The location and sequence was defaulted to the + strand.",
    "contig_length_feature": "This feature spans entire contig length.",
    "assembly_ref_extra_contigs": "The genbank file contains the following contigs which are not present "
                    "in the supplied assembly: {}",
    "assembly_ref_diff_seq": "The genbank file contains the following contigs which sequence does not match the "
                    "supplied assembly sequence: {}"
}


def get_start(loc):
    start = loc[1]
    strand = loc[2]
    leng = loc[3]
    if strand == '+':
        return start
    if strand == '-':
        return start - (leng - 1)
    return 0


def get_end(loc):
    start = loc[1]
    strand = loc[2]
    leng = loc[3]
    if strand == '+':
        return start + (leng - 1)
    if strand == '-':
        return start
    return 0


def get_bio_end(loc):
    if loc[2] == "+":
        return loc[1] + loc[3]
    else:
        return loc[1] - loc[3]


def is_parent(feat1, feat2):
    """Check if all locations in feat2 fall within a location in feat1"""

    def _contains(loc1, loc2):
        if loc1[0] != loc2[0]:  # different contigs
            return False
        if loc1[2] != loc2[2]:  # different strands
            return False
        elif loc1[2] == "+":
            return loc2[1] >= loc1[1] and (
                loc2[1] + loc2[3] <= loc1[1] + loc1[3])
        else:
            return loc2[1] <= loc1[1] and (
                loc2[1] - loc2[3] >= loc1[1] - loc1[3])

    j = 0
    for i, l2 in enumerate(feat2['location']):
        if j >= len(feat1['location']):
            logging.info(f"No part in {feat1['location']} contains {l2}")
            return False
        if feat1.get('type') == 'gene' or i == 0:
            while not _contains(feat1['location'][j], l2):
                j += 1
                if j == len(feat1['location']):
                    logging.info(f"No part in {feat1['location']} contains {l2}")
                    return False
            if feat1.get('type') == 'gene' or len(feat2['location']) == 1:
                continue

        l1 = feat1['location'][j]
        if i == 0 and get_bio_end(l2) != get_bio_end(l1):
            logging.info(
                f"For the first exon of the CDS the end sites must match: L1{l1} vs L2{l2}")
            return False
        elif i == len(feat2['location']) - 1 and l2[1] != l1[1]:
            logging.info(
                f"For the last exon of the CDS the start sites must match: L1{l1} vs L2{l2}")
            return False
        elif 0 < i < (len(feat2['location']) - 1) and l1 != l2:
            logging.info(f"For an interior exon all coordinates must match: L1{l1} vs L2{l2}")
            return False
        j += 1
    return True


def parse_inferences(inferences):
    """Whoever designed the genbank delimitation is an idiot: starts and
    ends with a optional values and uses a delimiter ":" that is
    used to divide it's DBs in the evidence. Anyway, this sorts that"""
    result = []
    for inf in inferences:
        try:
            sp_inf = inf.split(":")
            if sp_inf[0] in ('COORDINATES', 'DESCRIPTION', 'EXISTENCE'):
                inference = {'category': sp_inf.pop(0)}
            else:
                inference = {'category': ''}
            inference['type'] = sp_inf[0]
            inference['evidence'] = ":".join(sp_inf[1:])
            result.append(inference)
        # FIXME what is going on with this try/except block
        except IndexError('Unparseable inference string: ' + inf):
            continue
    return result


def propagate_cds_props_to_gene(cds, gene, is_metagenome=False):
    # Put longest protein_translation to gene
    if not is_metagenome:
        if "protein_translation" not in gene or (
                    len(gene["protein_translation"]) <
                    len(cds["protein_translation"])):
            gene["protein_translation"] = cds["protein_translation"]
            gene["protein_translation_length"] = len(
                cds["protein_translation"])
    # Merge cds list attributes with gene
    for key in ('functions', 'aliases', 'db_xref'):
        if cds.get(key, []):
            gene[key] = cds.get(key, []) + gene.get(key, [])
    # Merge cds["ontology_terms"] -> gene["ontology_terms"]
    terms2 = cds.get("ontology_terms")
    if terms2 is not None:
        terms = gene.get("ontology_terms")
        if terms is None:
            gene["ontology_terms"] = terms2
        else:
            for source in terms2:
                if source in terms:
                    terms[source].update(terms2[source])
                else:
                    terms[source] = terms2[source]


def load_ontology_mappings(path='data'):
    mapping_dict = {}
    for file in os.listdir(path):
        m = re.match(r"(\w+)_ontology_mapping.json", file)
        if m:
            ont_dict = json.load(open(os.path.join(path, file)))
            mapping_dict[m.group(1).upper()] = ont_dict
    if not mapping_dict:
        raise ValueError(f'No valid ontology mappings were found at {path}')
    logging.info(f'Loaded {len(mapping_dict)} ontologies')
    return mapping_dict


def check_full_contig_length_or_multi_strand_feature(feature, is_transpliced, contig_length, skip_types):
    """
    Tests for full contig length features and if on both strands.
    """
    feature_min_location = None
    feature_max_location = None
    strand_set = set()
    contig_id = feature["location"][0][0]
    for location in feature["location"]:
        if location[0] != contig_id:
            return feature
        location_min = get_start(location)
        location_max = get_end(location)
        strand_set.add(location[2])
        if feature_min_location is None or feature_min_location > location_min:
            feature_min_location = location_min
        if feature_max_location is None or feature_max_location < location_max:
            feature_max_location = location_max
    if feature_min_location == 1 \
            and feature_max_location == contig_length \
            and feature['type'] not in skip_types:
        feature["warnings"] = feature.get('warnings', []) + [warnings["contig_length_feature"]]
    if len(strand_set) > 1 and not is_transpliced:
        feature["warnings"] = feature.get('warnings', []) + [warnings["both_strand_coordinates"]]
    return feature


def check_feature_ids_uniqueness(genome):
    """
    Tests that all feature ids in a genome are unique across all 4 feature type lists
    Returns dict of Non Unique IDS and the counts associated with them.
    If all IDS are unique, it then returns an empty dict
    """
    unique_feature_ids = set()
    duplicate_feature_id_counts = dict()
    feature_lists = ["features", "cdss", "mrnas", "non_coding_features"]

    for feature_list in feature_lists:
        for feature in genome[feature_list]:
            if feature["id"] in unique_feature_ids:
                if feature["id"] in duplicate_feature_id_counts:
                    duplicate_feature_id_counts[feature["id"]] += 1
                else:
                    duplicate_feature_id_counts[feature["id"]] = 2
            else:
                unique_feature_ids.add(feature["id"])
    if len(unique_feature_ids) == 0:
        raise ValueError("Error no feature ids found in this genome")
    return duplicate_feature_id_counts


def make_id_set(feature_list):
    """
    Helper function to make id lookup sets for a feature list
    """
    return set((x['id'] for x in feature_list))


def confirm_feature_relationships(feature, feature_list_name, feature_id_sets_dict):
    """
    Pass in a feature and the list that the feature came from, it then
    verifies if all feature ids in the relationships are present
    Note it does not check if a relationship field is present as some features will not have determined relationships.
    returns dict of types as keys and ids that were not found. An empty dict means all declared relationships were found
    """
    not_found_relationships = dict()
    if len(feature_id_sets_dict) == 0:
        raise ValueError('feature_id_sets_dict is empty')
    if feature_list_name == "features":
        # means protein encoding gene may have mRNA and chidren relationships, should have CDS relationships.
        not_found_cdss = list()
        for cds in feature['cdss']:
            if cds not in feature_id_sets_dict['cdss']:
                not_found_cdss.append(cds)
        if len(not_found_cdss) > 0:
            not_found_relationships['cdss'] = not_found_cdss
        if "mrnas" in feature:
            not_found_mrnas = list()
            for mrna in feature['mrnas']:
                if mrna not in feature_id_sets_dict['mrnas']:
                    not_found_mrnas.append(mrna)
            if len(not_found_mrnas) > 0:
                not_found_relationships['mrnas'] = not_found_mrnas
        if "children" in feature:
            not_found_children = list()
            for child in feature['children']:
                if child not in feature_id_sets_dict['non_coding_features']:
                    not_found_children.append(child)
            if len(not_found_children) > 0:
                not_found_relationships['children'] = not_found_children
    elif feature_list_name == "cdss":
        # means will have parent_gene relationship, may have parent_mrna relationship.
        if "parent_gene" in feature:
            if feature['parent_gene'] not in feature_id_sets_dict['features']:
                not_found_relationships['parent_gene'] = [feature['parent_gene']]
        if "parent_mrna" in feature:
            if feature['parent_mrna'] not in feature_id_sets_dict['mrnas']:
                not_found_relationships['mrnas'] = [feature['parent_mrna']]
    elif feature_list_name == "mrnas":
        # means will have parent_gene relationship, may have CDS relationship.
        if "parent_gene" in feature:
            if feature['parent_gene'] not in feature_id_sets_dict['features']:
                not_found_relationships['parent_gene'] = [feature['parent_gene']]
        if "cds" in feature:
            if feature['cds'] not in feature_id_sets_dict['cdss']:
                not_found_relationships['cds'] = [feature['cds']]
    elif feature_list_name == "non_coding_features":
        # NEED TO CHECK BOTH FEATURES AND NON_CODING_FEATURES FOR PARENT_GENE
        # Children will only be NON_CODING_FEATURES
        # Do parent could be in either feature or non_coding_features (Only 1 parent)
        if "parent_gene" in feature:
            if feature['parent_gene'] not in feature_id_sets_dict['features'] and \
                    feature['parent_gene'] not in feature_id_sets_dict['non_coding_features']:
                not_found_relationships['parent_gene'] = [feature['parent_gene']]
        if "children" in feature:
            not_found_children = list()
            for child in feature['children']:
                if child not in feature_id_sets_dict['non_coding_features']:
                    not_found_children.append(child)
            if len(not_found_children) > 0:
                not_found_relationships['children'] = not_found_children
    else:
        # Raise an error the searched for feature does not exist in any of the 4 lists.
        raise ValueError('Feature List Name : ' + feature_list_name + ' was not one of the expected 4 types.')
    return not_found_relationships


def confirm_genomes_feature_relationships(genome):
    """
    Confirms the relationships of all features in a genome
    Note this is not a quick operation, should be used sparingly and not necessarily for every genome
    Takes a genome and returns a dict with feature ids as the key and a dict of relationship type and missing features
    NOTE THIS DOES NOT INSURE THAT RELATIONSHIPS ARE RECIPROCAL. JUST CHECKS
    THAT A FEATURE EXISTS FOR LISTED RELATIONSHIPS.
    """
    features_with_relationships_not_found = dict()
    feature_lists = ["features", "cdss", "mrnas", "non_coding_features"]
    # dict is the feature list and the key is the set of ids in that list.
    feature_id_sets_dict = dict()
    for feature_list in feature_lists:
        if feature_list in genome:
            feature_id_sets_dict[feature_list] = make_id_set(genome[feature_list])
        else:
            feature_id_sets_dict[feature_list] = set()
    for feature_list in feature_lists:
        for feature in genome[feature_list]:
            feature_relationship_dict = confirm_feature_relationships(feature, feature_list, feature_id_sets_dict)
            if len(feature_relationship_dict) > 0:
                features_with_relationships_not_found[feature['id']] = feature_relationship_dict
    return features_with_relationships_not_found


# James wrote this in AssemblyUtil, copying here.
def sort_dict(in_struct):
    """Recursively sort a dictionary by dictionary keys. (saves WS the trouble)"""
    if isinstance(in_struct, dict):
        return {k: sort_dict(in_struct[k]) for k in sorted(in_struct)}
    elif isinstance(in_struct, list):
        return [sort_dict(k) for k in in_struct]
        # return [sort_dict(k) for k in sorted(in_struct)]
    else:
        return in_struct


def set_default_taxon_data(genome_dict):
    """
    Add defaults to the genome data dict for taxonomy-related fields.
    This will not override any preset or user-passed fields.
    Mutates genome_dict.
    """
    sciname = genome_dict.get('sciname')
    if sciname:
        genome_dict.setdefault('taxonomy', f'Unconfirmed Organism: {sciname}')
    else:
        genome_dict.setdefault('taxonomy', 'Unconfirmed Organism')
    # XXX genetic_code should probably not have a default, but this matches previous/legacy behavior.
    genome_dict.setdefault('genetic_code', 11)
    genome_dict.setdefault('domain', 'Unknown')


def set_taxon_data(tax_id, re_api_url, genome_dict):
    """
    Fetch and set taxonomy data for a genome using an NCBI taxonomy ID.

    We mutate the genome_dict with the following fields:
    {
      "taxonomy": "x;y;z",    # NCBI taxonomy lineage string for human readability
      "domain": "x"           # String name of the domain
      "genetic_code": 11      # NCBI categorization of the lineage
                               (https://www.ncbi.nlm.nih.gov/Taxonomy/Utils/wprintgc.cgi)
      "taxon_assignments": {  # Mapping of taxonomy namespace to taxonomy ID
        "ncbi": 1234
      }
    }
    """
    # We use the Relation Engine to do a lookup on taxonomy data from ID
    re_client = REClient(re_api_url)
    # FIXME this timestamp should come from the client
    now = int(time.time() * 1000)  # unix epoch for right now, for use in the RE API
    tax_id = str(tax_id)
    genome_dict.setdefault('warnings', [])
    assignments = genome_dict.get('taxon_assignments')
    # Check to make sure that tax_id == genome.taxon_assignments.ncbi
    if assignments and assignments.get(_NCBI_TAX) and assignments[_NCBI_TAX] != tax_id:
        raise RuntimeError(
            f"The provided taxon ID ({tax_id}) differs from the "
            f"taxon ID in the genome's `taxon_assignments` field: {assignments[_NCBI_TAX]}."
        )
    genome_dict['taxon_assignments'] = {'ncbi': tax_id}
    # Fetch the taxon from Relation Engine by taxon ID
    try:
        resp_json = re_client.stored_query(
            'ncbi_fetch_taxon',
            {'id': str(tax_id), 'ts': now},
            raise_not_found=True)
    except RENotFound as err:
        # Taxon not found; log and raise
        logging.error(str(err))
        raise err
    re_result = resp_json['results'][0]
    # Refer to the following schema for returned fields in `re_result`:
    # https://github.com/kbase/relation_engine_spec/blob/develop/schemas/ncbi/ncbi_taxon.yaml
    gencode = int(re_result['gencode'])
    # If there is a mismatch on some of these fields from NCBI, save a warning and continue
    if genome_dict.get('genetic_code') and genome_dict['genetic_code'] != gencode:
        genome_dict['warnings'].append(
            f"The genetic code provided by NCBI ({gencode}) "
            f"does not match the one given by the user ({genome_dict['genetic_code']})"
        )
    genome_dict['genetic_code'] = gencode
    # Fetch the lineage on RE using the taxon ID to fill the "taxonomy" and "domain" fields.
    lineage_resp = re_client.stored_query(
        'ncbi_taxon_get_lineage',
        {'id': str(tax_id), 'ts': now, 'select': ['scientific_name', 'rank']},
        raise_not_found=True)
    # The results will be an array of taxon docs with "scientific_name" and "rank" fields
    lineage = [
        r['scientific_name'] for r in lineage_resp['results']
        if r['scientific_name'] != 'root'
    ]
    # Format and normalize the lineage string
    taxonomy = '; '.join(lineage).replace('\n', '')
    # Fetch the domain in the lineage. The `domain` var should be a singleton list.
    # In NCBI taxonomy, 'domain' is known as 'superkingdom'
    domain = [r['scientific_name'] for r in lineage_resp['results'] if r['rank'] == 'superkingdom']
    if genome_dict.get('domain') and genome_dict['domain'] != domain:
        genome_dict['warnings'].append(
            f"The domain provided by NCBI ({domain}) "
            f"does not match the one given by the user ({genome_dict['domain']})"
        )
    # Set the domain from NCBI, if possible. Otherwise, fall back to anything
    # the user supplied, or 'Unknown'.
    if domain:
        genome_dict['domain'] = domain[0]
    elif not genome_dict.get('domain'):
        genome_dict['domain'] = 'Unknown'
    genome_dict['taxonomy'] = taxonomy
    sciname = re_result['scientific_name']
    # The FastaGFFToGenome labyrinth of code sets the below default, which we want to override
    if genome_dict.get('scientific_name') and genome_dict['scientific_name'] != sciname:
        genome_dict['warnings'].append(
            f"The scientific name provided by NCBI ('{sciname}') "
            f"does not match the one given by the user ('{genome_dict['scientific_name']}')"
        )
    # Assign the scientific name to the most specific (right-most) taxon in the lineage
    genome_dict['scientific_name'] = sciname
 
