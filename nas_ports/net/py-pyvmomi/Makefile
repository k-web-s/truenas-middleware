PORTNAME=	pyvmomi
DISTVERSIONPREFIX=	v
DISTVERSION=	9.1.0.0
CATEGORIES=	net python

MAINTAINER=	robak@FreeBSD.org
COMMENT=	Python SDK for the VMware vSphere API of ESX, ESXi, and vCenter
WWW=		https://github.com/vmware/pyvmomi

USE_GITHUB=     yes
GH_ACCOUNT=     vmware
GH_PROJECT=	${PORTNAME}

LICENSE=	APACHE20

BUILD_DEPENDS=	${PYTHON_PKGNAMEPREFIX}setuptools>=0:devel/py-setuptools@${PY_FLAVOR} \
		${PYTHON_PKGNAMEPREFIX}wheel>=0:devel/py-wheel@${PY_FLAVOR}

RUN_DEPENDS=	${PYTHON_PKGNAMEPREFIX}requests>=2.3.0:www/py-requests@${PY_FLAVOR} \
		${PYTHON_PKGNAMEPREFIX}six>=1.7.3:devel/py-six@${PY_FLAVOR}

USES=		python
USE_PYTHON=	autoplist concurrent optsuffix pep517

.include <bsd.port.mk>
