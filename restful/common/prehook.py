#!/usr/bin/env python2
import audi
import audi.cdap as cdap
from audi.cdap import ClientRestClient


def cdap_deploy_app(jar_path):
    client = ClientRestClient()
    resp = client.deploy_app(jar_path)
    if resp.status_code != 200:
        err = 'Failed to deploy [{0}]!\n'.format(jar_path)
        err += '{0}: {1}'.format(resp.status_code, resp.content)
        raise RuntimeError(err)


print 'running pre-hook'
cdap.build_examples(audi.config['cdap'].get('branch', 'develop'))

# deploy apps
cdap_examples = audi.config['cdap']['examples']
cdap_deploy_app(cdap_examples['Purchase'])
cdap_deploy_app(cdap_examples['HelloWorld'])
cdap_deploy_app(cdap_examples['WordCount'])
