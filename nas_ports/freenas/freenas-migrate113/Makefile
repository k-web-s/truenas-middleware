# $FreeBSD$

PORTNAME=	freenas-migrate113
PORTVERSION=	2026082000
CATEGORIES=	freenas
VALID_CATEGORIES+=	freenas

MAINTAINER=	dev@ixsystems.com
COMMENT=	Tool to upgrade to 11.3 database

RUN_DEPENDS=	python>0:${PORTSDIR}/lang/python \
		${PYTHON_PKGNAMEPREFIX}sqlparse>0:databases/py-sqlparse@${PY_FLAVOR}

USES=		python
USE_GITHUB=	yes
GH_ACCOUNT=	dravanet
GH_TAGNAME=	0adda07426

NO_BUILD=	yes

.include <bsd.port.pre.mk>

# cgi module was removed from the stdlib in Python 3.13
.if ${PYTHON_REL} >= 31300
RUN_DEPENDS+=	${PYTHON_PKGNAMEPREFIX}legacy-cgi>0:www/py-legacy-cgi@${PY_FLAVOR}
.endif

do-install:
	${MKDIR} ${STAGEDIR}${PREFIX}/lib/migrate113
	cd ${WRKSRC} && ${COPYTREE_SHARE} django ${STAGEDIR}${PREFIX}/lib/migrate113/
	cd ${WRKSRC} && ${COPYTREE_SHARE} freenasUI ${STAGEDIR}${PREFIX}/lib/migrate113/
	cd ${WRKSRC} && ${COPYTREE_SHARE} src ${STAGEDIR}${PREFIX}/lib/migrate113/
	${SED} -e "s,/usr/lib/,/usr/local/lib/,g" -e "s,/usr/bin/,/usr/local/bin/,g" \
		${WRKSRC}/sbin/migrate113 > ${STAGEDIR}${PREFIX}/sbin/migrate113
	${CHMOD} a+x ${STAGEDIR}${PREFIX}/sbin/migrate113
	(cd ${STAGEDIR}${PREFIX}/lib/migrate113; ${FIND} . -type f \
		| ${SED} -e 's,^\./,,g' \
		| ${AWK} '{print length, $$0}' | ${SORT} -rn \
		| ${AWK} '{print "lib/migrate113/"$$2 }' >> ${TMPPLIST})

.include <bsd.port.post.mk>
