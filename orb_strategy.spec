# -*- mode: python ; coding: utf-8 -*-
# orb_strategy.spec — PyInstaller spec for ORB 5-Min Strategy EXE

block_cipher = None

a = Analysis(
    ['orb_5m_strategy.py'],
    pathex=['.'],
    binaries=[],
    datas=[],
    hiddenimports=[
        'websocket',
        'websocket._app',
        'websocket._core',
        'websocket._exceptions',
        'websocket._handshake',
        'websocket._http',
        'websocket._logging',
        'websocket._socket',
        'websocket._ssl_compat',
        'websocket._utils',
        'pandas',
        'pandas._libs',
        'pandas._libs.tslibs',
        'pandas.core',
        'pyotp',
        'schedule',
        'requests',
        'dotenv',
        'dhan_token_manager',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'scipy', 'PIL', 'cv2', 'PyQt5', 'PyQt6'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ORB_5Min_Strategy',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
