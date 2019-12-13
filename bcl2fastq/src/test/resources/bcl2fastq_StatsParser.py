#!/usr/bin/python

from __future__ import division

import json
import os
import sys

def StatsParser():
  if not os.path.isfile('/data/Data/Intensities/BaseCalls/Stats/Stats.json'):
    raise ValueError("Stats file not found")
    sys.exit(1)

  with open('/data/Data/Intensities/BaseCalls/Stats/Stats.json') as file:
    stats = json.load(file)

  flowcell = {}
  lanes = []
  samples = {}
  fastqs = []

  flowcell['id'] = stats['Flowcell']
  flowcell['q30'] = 0
  flowcell['yld'] = 0
  flowcell['undet_rds'] = 0

  for lane in stats['ConversionResults']:

    lane_q30 = 0
    lane_yld = 0

    if 'Undetermined' not in samples:
      samples['Undetermined'] = {
        'id': 'Undetermined',
        'yld': 0,
        'q30': 0
      }

    if 'Undetermined' in lane:
      for read in lane['Undetermined']['ReadMetrics']:
        samples['Undetermined']['yld'] += read['Yield']
        samples['Undetermined']['q30'] += read['YieldQ30']

        lane_yld += read['Yield']
        lane_q30 += read['YieldQ30']

    for sample in lane['DemuxResults']:

      if sample['SampleId'] not in samples:
        samples[sample['SampleId']] = {
          'name': sample['SampleName'],
          'id':   sample['SampleId'],
          'yld':  0,
          'q30':  0
        }

      reads_yld = 0
      reads_q30 = 0

      for read in sample['ReadMetrics']:
        reads_yld += read['Yield']
        reads_q30 += read['YieldQ30']
    
      fastqs.append({
        'id':    sample['SampleId'],
        'lane':  'L00' + str(lane['LaneNumber']),
        'yld':   reads_yld,
        'q30':   reads_q30 / reads_yld * 100 if reads_yld > 0 else 0
      })

      samples[sample['SampleId']]['yld'] += reads_yld
      samples[sample['SampleId']]['q30'] += reads_q30

      lane_yld += reads_yld
      lane_q30 += reads_q30

    lanes.append({
      'id':  lane['LaneNumber'],
      'yld': lane_yld,
      'q30': lane_q30
    })

    flowcell['yld'] += lane_yld
    flowcell['q30'] += lane_q30

  flowcell['q30'] = flowcell['q30'] / flowcell['yld'] * 100 if flowcell['yld'] > 0 else 0

  for sample in samples:
    samples[sample]['q30'] = samples[sample]['q30'] / samples[sample]['yld'] * 100 if samples[sample]['yld'] > 0 else 0

  for lane in range(len(lanes)):
    lanes[lane]['q30'] = lanes[lane]['q30'] / lanes[lane]['yld'] * 100 if lanes[lane]['yld'] > 0 else 0

  with open('/data/fastq-stats.csv', 'w') as file:
    file.write(','.join(['Flowcell', str(flowcell['id']), str(flowcell['yld']), str(flowcell['q30'])]) + '\n')

    for lane in lanes:
      file.write(','.join(['Lane', 'L00' + str(lane['id']), str(lane['yld']), str(lane['q30'])]) + '\n')

    for sample in samples:
      if sample != 'Undetermined':
        file.write(','.join(['Sample', sample, str(samples[sample]['yld']), str(samples[sample]['q30'])]) + '\n')

    file.write(','.join(['Sample', 'Undetermined', str(samples['Undetermined']['yld']), str(samples['Undetermined']['q30'])]) + '\n')

    for fastq in fastqs:
      file.write(','.join([fastq['id'], fastq['lane'], str(fastq['yld']), str(fastq['q30'])]) + '\n')
