'''
Created on Jan 15, 2010

@author: acs
'''
marnyTask = Task(name="RE-sq19sh20-samID_msk-v001",
                 arguments={'cam': 'actor:cam_sq19sh20',
                            'p':'RE-sq19sh20-samID_msk-v001',
                            's': 858,
                            'e': 900,
                            'ass': 1,
                            'assf': "/tmp/assfiles",
                            'proj': '/s/v/LAM',
                            'o': 12,
                            'rx': 768,
                            'ry': 328,
                            'f': '/s/q/DevImages/acs/images/render_test_marny_puli/test2',
                            'scene': '/s/v/LAM/scenes/sq19/sh20/render/wip/hdebat/samID_msk/RE-sq19sh20-samID_msk-v001.ma',
                            'pad': 4,
                            'packetSize': 4,
                            'arnold': 'mArny2.27.21',
                            'maya': '2009',
                            'nuke': '5.1v3',
                            'after': '7.0',
                            'shave': 'shaveHaircut-5.1v2'},
                 tags={"prod": "LAM", "plan": "sq19sh20"},
                 decomposer="puliclient.contrib.arnold.ArnoldDecomposer")

submit(marnyTask)
