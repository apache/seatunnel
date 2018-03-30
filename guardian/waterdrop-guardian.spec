# -*- mode: python -*-
a = Analysis(['waterdrop-guardian.py'],
             pathex=['/Users/yixia/IdeaProjects/waterdrop/guardian'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None)
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='waterdrop-guardian',
          debug=False,
          strip=None,
          upx=True,
          console=True )
