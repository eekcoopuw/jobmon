import pytest
from time import sleep
import os
import typing

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

# The testing pub file
_PUB_FILE = "test_rsa.pub"
# The testing auth file
_AUTH_FILE = "test_auth"

# Unit test for _key_in_auth_keyfile
def test_key_in_auth_keyfile():
    dir = os.getcwd()
    pub_path = dir + "/" + _PUB_FILE
    auth_path = dir + "/" + _AUTH_FILE
    # Test pub key not in auth file
    f = open(pub_path, "w")
    f.write(_KEY1)
    f.close()
    f = open(auth_path, "w")
    f.write(_KEY2)
    f.close()
    assert(not utils._key_in_auth_keyfile(pub_path, auth_path))
    # Test pub key in auth file
    os.remove(auth_path)
    f = open(auth_path, "w")
    f.write(_KEY1)
    f.close()
    assert(utils._key_in_auth_keyfile(pub_path, auth_path))
    # Clean the test files
    os.remove(pub_path)
    os.remove(auth_path)


