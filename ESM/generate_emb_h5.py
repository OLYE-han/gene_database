import argparse
import pathlib
import string
import torch
import esm
from esm import Alphabet, FastaBatchedDataset, ProteinBertModel, pretrained, MSATransformer
import pandas as pd
from tqdm import tqdm
from Bio import SeqIO
import itertools
from typing import List, Tuple
import numpy as np
import h5py
import os


def main(arg):
    os.environ['CUDA_VISIBLE_DEVICES']= arg.gpuid
    # prepare your protein sequences/structures as a list.
    Seq_dict = {}
    name = ''
    seq = ''
    with open(arg.input, 'r') as f:
        for l in f.readlines():
            if l.startswith('>'):
                if len(name) != 0 :
                    Seq_dict[name] = seq
                    seq = ''
                    name = ''
                name = l.strip()[1:]
            else:
                seq += l.strip().upper().replace(' ', '')

    Seq_dict[name] = seq
    print(len(Seq_dict))
    device = torch.device("cuda:0")
    dsize = 1280
    if arg.model == 'ESM1b':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm1b_t33_650M_UR50S()
    elif arg.model == 'ESM2_3B':
        rep_layer = 36
        model, alphabet = esm.pretrained.esm2_t36_3B_UR50D()
        dsize = 2560
    elif arg.model == 'ESM2_650M':
        rep_layer = 33
        model, alphabet = esm.pretrained.esm2_t33_650M_UR50D()
    else:
        print('Unknown model')
        exit()

    batch_converter = alphabet.get_batch_converter()
    model.to(device)

    total_emb = []
    file_name = []
    seq_len = []
    for i,seq_name in enumerate(Seq_dict.keys()):
        plen = min(len(Seq_dict[seq_name]),1022)
        pseq = Seq_dict[seq_name][:plen]

        data = [('p', pseq)]
        masked_tokens, masked_pos, batch_tokens = batch_converter(data)
        batch_tokens = batch_tokens.to(device)
        with torch.no_grad():
            results = model(batch_tokens, repr_layers=[rep_layer], return_contacts=True)
        token_representations = results["representations"][rep_layer].squeeze(0)
        #mean token_representations
        token_representations = np.mean(token_representations.cpu().numpy()[1:-1], axis = 0)
        #all token_representations
        #token_representations = token_representations.cpu().numpy()[1:-1]
        total_emb.append(token_representations)
        file_name.append(seq_name.encode('utf-8'))
        seq_len.append(plen)
        if(i % 100 == 0):
            print('Processing %d:%d'%(i, len(Seq_dict)))

    write_data(total_emb, file_name, seq_len, dsize)


def write_data(total_emb, p_name, p_len, dsize):
    if arg.output != None:
        fname = arg.output
    else:
        fname = 'emb.h5'
    
    f = h5py.File(fname, 'w')
    print(np.array(total_emb, dtype='float32').shape)
    f.create_dataset('embeddings', data=np.array(total_emb, dtype='float32'), compression="gzip", chunks=True, maxshape=(None,dsize)) 
    f.create_dataset('Seq_Name', data=np.array(p_name), compression="gzip", chunks=True, maxshape=(1000000)) 
    f.create_dataset('seq_len', data=np.array(p_len), compression="gzip", chunks=True, maxshape=(1000000)) 
    f.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Config file')
    parser.add_argument('-i', '--input', help='path to input fasta file', required=True)
    parser.add_argument('-g', '--gpuid', help='id of gpu', default = '0', required=False)
    parser.add_argument('-o', '--output', help='output path', default = 'embedding.h5', required=False)
    parser.add_argument('-m', '--model', help='pre-trained model to used', default = 'ESM1b', required=False)
    arg = parser.parse_args()
    main(arg)

# python generate_emb_h5.py -i SMAGs.fasta -m ESM2_3B