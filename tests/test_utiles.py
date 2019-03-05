import pytest
import time
import os

from jobmon.client import utils


# The RSA public key used to create testing test_rsa.pub file
_KEY1 = "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEA0HDKZ8Kes1U0l3+sGTkdATbADSzXJtozIHL7wxInln05udzixpEs4LPyUKPi584W6+" + \
        "WQkvkkketCy+kDm9oIm4dKAdonv66P5Db/354k1TTNC49kimJc69K+kzWOiIjMD5tsiRcqwiz9B4GBQ9KlyiN2MByNANX9O4bScQGk" + \
        "IOGni/dhn0Txh33xzFd8EXdXrgQKob/KC4LfV46TT3bacNMVqJURPuo4zX62K9o8fnIx5hk27+V13EYs/TcNh18QhepWHc2lLHDIxW" + \
        "rXsRQydD/obxchunlyHyroCMDnex0iQfdpUgLTzBXHn5b3g4qc0gQaF3t6zR0u027stOWtyw== limingxu@cn461.ihme.washington.edu"

# The string used to create test_auth file
_KEY2 = "ssh-rsa AAAAB3NzaC1yc2EPPPBIwDDDQEA0HDKZ8Kes1U0l3+sGTkdATbADSzXJtozIHL7wxInln05udzixpEs4LPyUKPi584W6+" + \
        "WQkvkkketCy+kDm9oIm4dKAdonv66P5Db/354k1TTNC49kimJc69K+kzWOiIjMD5tsiRcqwiz9B4GBQ9KlyiN2MByNANX9O4bScQGk" + \
        "IOGni/dhn0Txh33xzFd8EXdXrgQKob/KC7LfV46TT3bacNMVqJURPuo4zX62K9o8fnIx5hk27+V13EYs/TcNh18QhepWHc2lLHDIxW" + \
        "rXsRQydD/obxchunlyHyroCMDnex0iQfdpUgLTzBXHn5b3g4qc0gQaF3t6zR0u027stOWtyw== limingxu@cn461.ihme.washington.edu"

# The testing key files
_PRI_FILE = "{d}/test_{t}_rsa".format(d=os.getcwd(), t=str(int(time.time())))
_PUB_FILE = "{}.pub".format(_PRI_FILE)
# The testing auth file
_AUTH_FILE = "{d}/test_{t}_auth".format(d=os.getcwd(), t=str(int(time.time())))
# The testing auth2 file
_AUTH_FILE2 = "{d}/test_{t}_auth2".format(d=os.getcwd(), t=str(int(time.time())))


# Unit test for _key_in_auth_keyfile
@pytest.mark.unittest
def test_key_in_auth_keyfile():
    # Test pub key not in auth file
    f = open(_PUB_FILE, "w")
    f.write(_KEY1)
    f.close()
    f = open(_AUTH_FILE, "w")
    f.write(_KEY2)
    f.close()
    r = utils._key_in_auth_keyfile(_PUB_FILE, _AUTH_FILE)
    # Clean the test files
    os.remove(_PUB_FILE)
    os.remove(_AUTH_FILE)
    assert(not r)
    # Test pub key in auth file
    f = open(_PUB_FILE, "w")
    f.write(_KEY1)
    f.close()
    f = open(_AUTH_FILE, "w")
    f.write(_KEY1)
    f.close()
    r = utils._key_in_auth_keyfile(_PUB_FILE, _AUTH_FILE)
    # Clean the test files
    os.remove(_PUB_FILE)
    os.remove(_AUTH_FILE)
    assert r


@pytest.mark.unittest
def test_set_authorized_keys_perms():
    f = open(_AUTH_FILE, "w")
    f.close()
    auth_files = [_AUTH_FILE]
    utils._set_authorized_keys_perms(auth_files)
    r = oct(os.stat(_AUTH_FILE).st_mode)[-3:]
    os.remove(_AUTH_FILE)
    assert(r == "600")


@pytest.mark.unittest
def test_add_keyfile_to_authorized_keys():
    f = open(_AUTH_FILE, "w")
    f.close()
    f = open(_PUB_FILE, "w")
    f.write(_KEY1)
    f.close()
    utils._add_keyfile_to_authorized_keys(_PRI_FILE, [_AUTH_FILE])
    r = utils._key_in_auth_keyfile(_PUB_FILE, _AUTH_FILE)
    os.remove(_AUTH_FILE)
    os.remove(_PUB_FILE)
    assert r


