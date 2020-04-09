-- Destination File Parmeters
set echo        off;
set verify      off;
set term        off;
set feed        off;
SET LINESIZE 10000;
SET COLSEP "|";
set heading Off;
set pagesize 0;
set prompt off;
set newpage 0;
set space 0;
set trimspool on;

-- Main Query: listing all postraheader
spool /arch/batch/BIG_DATA/ART/ART_MASTER.TXT;
-- Main Query: listing sites
select
art_code||chr(124)||
art_desc||chr(124)||
brand_desc||chr(124)||
art_type||chr(124)||
(case when div_code in ('A','B','C','D','E') then 'FS'
when div_code in ('J','K','L','O','Q','T','U') then 'SPM'
when div_code in ('S') then 'YB'
when div_code in ('N','Q') then 'SBU'
when div_code in ('P') then 'FC'
else 'LL' end)  ||chr(124)||
(case when div_code in ('A','B','C','D','E') then 'FASHION'
when div_code in ('J','K','L','O','Q','T','U') then 'SUPERMARKET'
when div_code in ('S') then 'YOA BUSANA'
when div_code in ('N','Q') then 'SBU'
when div_code in ('P') then 'FOOD COURT'
else 'LL' end) ||chr(124)||
else 'LL' end) ||chr(124)||
DIV_CODE||chr(124)||
PKSTRUCOBJ.GET_DESC2(DIV_CODE,'GB')||chr(124)||
CAT_CODE||chr(124)||
PKSTRUCOBJ.GET_DESC2(CAT_CODE,'GB')||chr(124)||
SUBCAT_CODE||chr(124)||
PKSTRUCOBJ.GET_DESC2(SUBCAT_CODE,'GB')||chr(124)||
CLASS_CODE||chr(124)||
PKSTRUCOBJ.GET_DESC2(CLASS_CODE,'GB')||chr(124)||
SUBCLASS_CODE||chr(124)||
PKSTRUCOBJ.GET_DESC2(SUBCLASS_CODE,'GB')
from
(
select artcexr art_code,
PKSTRUCOBJ.GET_DESC(artcinr,'GB') art_desc,
YG_AS_PKG.GET_BRAND_BY_CEXR(artcexr) brand_desc,
arttypp art_type,
YG_AS_PKG.GET_MS_BY_ART(ARTCEXR,1) DIV_CODE,
YG_AS_PKG.GET_MS_BY_ART(ARTCEXR,2) CAT_CODE,
YG_AS_PKG.GET_MS_BY_ART(ARTCEXR,3) SUBCAT_CODE,
YG_AS_PKG.GET_MS_BY_ART(ARTCEXR,4) CLASS_CODE,
YG_AS_PKG.GET_MS_BY_ART(ARTCEXR,6) SUBCLASS_CODE
from artrac
);
/
exit;
