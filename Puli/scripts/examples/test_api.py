from __future__ import absolute_import
import requests

from puliclient.server import RenderNodeHandler

rnh = RenderNodeHandler()
rn = rnh.renderNode( "vfxpc64:8004" )

print rn