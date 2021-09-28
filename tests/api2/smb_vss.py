#!/usr/bin/env python3

import pytest
import sys
import os
from subprocess import run
from time import sleep
apifolder = os.getcwd()
sys.path.append(apifolder)
from functions import PUT, POST, GET, DELETE, SSH_TEST
from auto_config import (
    ip,
    pool_name,
    user,
    password,
)
from pytest_dependency import depends
from protocols import SMB
from samba import ntstatus


vs_dataset = f"{pool_name}/smb-vss"
vs_dataset_url = vs_dataset.replace('/', '%2F')
vs_dataset_nested = f"{vs_dataset}/sub1"
vs_dataset_nested_url = vs_dataset_nested.replace('/', '%2F')

VSS_SMB_NAME = "SMBVSS"
vss_smb_path = "/mnt/" + vs_dataset

VSS_SMB_USER = "smbshadowuser"
VSS_SMB_PWD = "smb1234"

to_check = [
    'testfile1',
    f'{VSS_SMB_USER}/testfile2',
    'sub1/testfile3'
]

snapshots = {
    'snapshot1': {'gmt_string': '', 'offset': 18},
    'snapshot2': {'gmt_string': '', 'offset': 36},
    'snapshot3': {'gmt_string': '', 'offset': 54},
}


def check_previous_version_exists(path, home=False):
    cmd = [
        'smbclient',
        f'//{ip}/{VSS_SMB_NAME if not home else VSS_SMB_USER}',
        '-U', f'{VSS_SMB_USER}%{VSS_SMB_PWD}',
        '-c' f'open {path}'
    ]
    cli_open = run(cmd, capture_output=True)
    if cli_open.returncode != 0:
        return (
            ntstatus.NT_STATUS_FAIL_CHECK,
            'NT_STATUS_FAIL_CHECK',
            cli_open.stderr.decode()
        )

    cli_output = cli_open.stdout.decode().strip()
    if 'NT_STATUS_' not in cli_output:
        return (0, 'NT_STATUS_OK', cli_output)

    cli_output = cli_output.rsplit(' ', 1)

    return (
        ntstatus.__getattribute__(cli_output[1]),
        cli_output[1],
        cli_output[0]
    )


"""
def check_previous_version_contents(path, contents, offset):
    cmd = [
        'smbclient',
        f'//{ip}/{VSS_SMB_NAME}',
        '-U', f'{VSS_SMB_USER}%{VSS_SMB_PWD}',
        '-c' f'prompt OFF; mget {path}'
    ]
    cli_get = run(cmd, capture_output=True)
    if cli_get.returncode != 0:
        return (
            ntstatus.NT_STATUS_FAIL_CHECK,
            'NT_STATUS_FAIL_CHECK',
            cli_open.stderr.decode()
        )

    cli_output = cli_get.stdout.decode().strip()
    if 'NT_STATUS_' in cli_output:
        cli_output = cli_output.rsplit(' ', 1)
        return (
            ntstatus.__getattribute__(cli_output[1]),
            cli_output[0]
        )

    with open(path[25:], "rb") as f:
        bytes = f.read()

    to_check = bytes[offset:]
    assert len(to_check) == 9, f'path: {path}, contents: {to_check.decode()}'
    os.unlink(path[25:])
    assert to_check.decode() == contents, path
    return (0, )
"""


@pytest.mark.parametrize('theds', [vs_dataset, vs_dataset_nested])
@pytest.mark.dependency(name="VSS_DATASET_CREATED")
def test_001_creating_smb_dataset(request, theds):
    payload = {
        "name": theds,
        "share_type": "SMB"
    }
    results = POST("/pool/dataset/", payload)
    assert results.status_code == 200, results.text
    result = POST("/zfs/snapshot/", {
        "dataset": theds,
        "name": "init",
    })
    assert result.status_code == 200, results.text

    result = GET(f"/zfs/snapshot/?id={theds}@init")
    assert result.status_code == 200, results.text
    assert len(result.json()) == 1


@pytest.mark.dependency(name="VSS_USER_CREATED")
def test_002_creating_shareuser_to_test_acls(request):
    depends(request, ['VSS_DATASET_CREATED'])

    global smbvssuser_id
    results = GET('/user/get_next_uid/')
    assert results.status_code == 200, results.text
    next_uid = results.json()

    payload = {
        "username": VSS_SMB_USER,
        "full_name": "SMB User",
        "group_create": True,
        "password": VSS_SMB_PWD,
        "uid": next_uid,
    }
    results = POST("/user/", payload)
    assert results.status_code == 200, results.text
    global vssuser_id
    vssuser_id = results.json()


@pytest.mark.dependency(name="VSS_SHARE_CREATED")
def test_003_creating_a_smb_share_path(request):
    depends(request, ["VSS_DATASET_CREATED"])
    global smb_id
    payload = {
        "comment": "SMB VSS Testing Share",
        "path": vss_smb_path,
        "name": VSS_SMB_NAME,
        "purpose": "NO_PRESET",
        "auxsmbconf": "shadow:ignore_empty_snaps = no",
    }
    results = POST("/sharing/smb/", payload)
    assert results.status_code == 200, results.text
    smb_id = results.json()['id']

    cmd = f'mkdir {vss_smb_path}/{VSS_SMB_USER}; zpool sync'
    results = SSH_TEST(cmd, user, password, ip)
    assert results['result'] is True, {"cmd": cmd, "res": results['output']}


@pytest.mark.dependency(name="VSS_SMB_SERVICE_STARTED")
def test_004_starting_cifs_service(request):
    depends(request, ["VSS_SHARE_CREATED"])
    payload = {"service": "cifs"}
    results = POST("/service/restart/", payload)
    assert results.status_code == 200, results.text


@pytest.mark.dependency(name="VSS_SMB1_ENABLED")
def test_005_enable_smb1(request):
    depends(request, ["VSS_SHARE_CREATED"])
    payload = {
        "enable_smb1": True,
        "guest": "nobody",
        "smb_options": "log level = 8 shadowzfs:10"
    }
    results = PUT("/smb/", payload)
    assert results.status_code == 200, results.text


@pytest.mark.dependency(name="SHARE_HAS_SHADOW_COPIES")
@pytest.mark.parametrize('proto', ["SMB1", "SMB2"])
def test_006_check_shadow_copies(request, proto):
    """
    This is very basic validation of presence of snapshot
    over SMB1 and SMB2/3.
    """
    depends(request, ["VSS_USER_CREATED"])
    sleep(5)
    c = SMB()
    snaps = c.get_shadow_copies(
        host=ip,
        share=VSS_SMB_NAME,
        username=VSS_SMB_USER,
        password=VSS_SMB_PWD,
        smb1=(proto == "SMB1")
    )
    assert len(snaps) == 1, snaps
