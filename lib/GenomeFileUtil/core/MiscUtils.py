"""
This file contains miscellaneous utilities that are not tied to the concept of a genome.
"""


def validate_lists_have_same_elements(l1, l2):
    """
    Given two lists/sets of values (from different sources), verify that they match up.
    
    Useful for comparing ids in GFF and Fasta files, or across Genomes and Assemblies.
    """
    diff = set(l1) ^ (set(l2))  # get the symmetric difference of the sets
    # check if all ids are shared
    return len(diff) == 0
