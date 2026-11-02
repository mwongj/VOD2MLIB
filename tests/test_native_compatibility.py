"""Opt-in compatibility checks against unmodified, externally stored native sources."""
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

PINS = {
    'min': ('1bddebdae153e418f9ea28eeb1ed00c1116939b6', '37b003b01ffdb8e00a46f15efc835a3a34ac9988884192ed83eb35563765fd60'),
    'current': ('065db17c5baf34d23ad89e46843857adc2a3c9b3', '19b48870cdf496fc8077d502c3f56376741896cd73ebc3a6a122dd94ac3ccc88'),
}


@pytest.mark.parametrize('version', ['min', 'current'])
def test_pinned_dispatcharr_population_compatibility(version):
    directory = os.environ.get('VOD2MLIB_NATIVE_COMPAT_SOURCE_DIR')
    if not directory:
        pytest.skip('Set VOD2MLIB_NATIVE_COMPAT_SOURCE_DIR to downloaded pinned native sources; see README')
    source = Path(directory) / f'vod_tasks_{version}.py'
    assert hashlib.sha256(source.read_bytes()).hexdigest() == PINS[version][1], 'Native source does not match pinned commit'
    harness = Path(__file__).parent / 'fixtures' / 'native_harness.py'
    result = subprocess.run([sys.executable, str(harness), version, str(source)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
