IF EXISTS (select NAME from sysobjects where NAME='spImportPopustaCSV' and TYPE='P')
    DROP PROC spImportPopustaCSV;
GO

CREATE PROCEDURE dbo.spImportPopustaCSV
    @Tvrtka VARCHAR(13),
    @Operater VARCHAR(20),
    @Datum VARCHAR(8),
    @Putanja VARCHAR(200)
with encryption
as

declare @rkd_apl_ozn varchar(3) = 'AD2'
declare @rkd_evp_sif varchar(10)
declare @evp_datod varchar(8) = dbo.SQLDatum(getdate())
declare @evp_vrijemod varchar(8) = cast(datepart(hh, getdate()) as varchar(2)) + ':' + dbo.vodeci(cast(datepart(mm, getdate()) as varchar(2)), 2, '0') + ':' + dbo.vodeci(cast(datepart(ss, getdate()) as varchar(2)), 2, '0')
select @rkd_evp_sif = dbo.Vodeci(isnull(max(cast(evp_sif as int)),0)+1, 10, '_') from evpren where isnumeric(evp_sif) = 1 and apl_ozn = @rkd_apl_ozn

begin

set nocount on
set ansi_warnings off

    CREATE TABLE #TempCSVData 
    (
        [ItemNo_] VARCHAR(100) COLLATE database_default,
        [Description] VARCHAR(400) COLLATE database_default,
        [K4] VARCHAR(200) COLLATE database_default,
        [K5] VARCHAR(200) COLLATE database_default,
        [K6] VARCHAR(200) COLLATE database_default,
        [Loyalty] VARCHAR(200) COLLATE database_default
    )

    DECLARE @SQL NVARCHAR(MAX)
    SET @SQL = '
        BULK INSERT #TempCSVData
        from ''' + @Putanja + '''
        WITH (
            FIELDTERMINATOR = '';'',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2
        )'
    EXEC sp_executesql @SQL

	update #TempCSVData
    set 
        [K4] = replace([K4], '%', ''),
        [K5] = replace([K5], '%', ''),
        [K6] = replace([K6], '%', ''),
        [Loyalty] = replace([Loyalty], '%', '')

declare @azuriranipopust int = 0

update apopust
set 
    apo_popust = 
        case
            when pag_sif = 4 and isnumeric(replace(x.K4, '%', '')) = 1 
                then convert(numeric(7, 3), case when isnumeric(replace(x.K4, '%', '')) = 1 then x.K4 else '0' end)
            when pag_sif = 5 and isnumeric(replace(x.K5, '%', '')) = 1 
                then convert(numeric(7, 3), case when isnumeric(replace(x.K5, '%', '')) = 1 then x.K5 else '0' end)
            when pag_sif = 6 and isnumeric(replace(x.K6, '%', '')) = 1 
                then convert(numeric(7, 3), case when isnumeric(replace(x.K6, '%', '')) = 1 then x.K6 else '0' end)
            when apo_klk = '%A' and isnumeric(replace(x.Loyalty, '%', '')) = 1 
                then convert(numeric(7, 3), case when isnumeric(replace(x.Loyalty, '%', '')) = 1 then x.Loyalty else '0' end)
            else '0'
        end, datum = @Datum, operater = @Operater
from apopust
inner join artikl art on apopust.art_sif0 = art.art_sif0
inner join #TempCSVData x on art.art_sif2 = x.ItemNo_
where apopust.tvr_sif = @Tvrtka 
    and ((pag_sif = 4 and isnumeric(replace(x.K4, '%', '')) = 1 and convert(numeric(7, 3), case when isnumeric(replace(x.K4, '%', '')) = 1 then x.K4 else '0' end) <> apopust.apo_popust)
        or (pag_sif = 5 and isnumeric(replace(x.K5, '%', '')) = 1 and convert(numeric(7, 3), case when isnumeric(replace(x.K5, '%', '')) = 1 then x.K5 else '0' end) <> apopust.apo_popust)
        or (pag_sif = 6 and isnumeric(replace(x.K6, '%', '')) = 1 and convert(numeric(7, 3), case when isnumeric(replace(x.K6, '%', '')) = 1 then x.K6 else '0' end) <> apopust.apo_popust)
	    or (apo_klk = '%A' and isnumeric(replace(x.Loyalty, '%', '')) = 1 and convert(numeric(7, 3), case when isnumeric(replace(x.Loyalty, '%', '')) = 1 then x.Loyalty else '0' end) <> apopust.apo_popust))

set @azuriranipopust += @@ROWCOUNT


declare @novipopust int = 0

insert into apopust (tvr_sif, apo_rb, apk_sif, apv_sif, pag_sif, art_sif0, apo_popust, apo_klk, apo_iznos_jmj, datum, operater)
select tvr_sif, row_number() over (partition by a.apo_rb order by a.art_sif0) as Redni_broj, apk_sif, apv_sif, pag_sif, art_sif0, apo_popust, apo_klk, apo_iznos_jmj, datum, operater from
(
	select 
		@Tvrtka as tvr_sif, ap.apo_rb, '1' as apk_sif,'1' as apv_sif, '4' as pag_sif, art.art_sif0, 
		case when isnumeric(replace(x.K4, '%', '')) = 1 then convert(numeric(7, 3), replace(x.K4, '%', '')) else 0 end as apo_popust,
		'' as apo_klk, 'N' as apo_iznos_jmj, @Datum as datum, @Operater as operater
	from artikl art
	inner join #TempCSVData x on art.art_sif2 = x.ItemNo_
	left join apopust ap on ap.art_sif0 = art.art_sif0
	where ap.art_sif0 is null and art.art_sif0 is not null and isnumeric(replace(x.K4, '%', '')) = 1

	union

	select 
		@Tvrtka as tvr_sif, ap.apo_rb, '1' as apk_sif, '1' as apv_sif, '5' as pag_sif, art.art_sif0, 
		case when isnumeric(replace(x.K5, '%', '')) = 1  then convert(numeric(7, 3), replace(x.K5, '%', '')) else 0 end as apo_popust,
		 '' as apo_klk, 'N' as apo_iznos_jmj, @Datum as datum, @Operater as operater
	from artikl art
	inner join #TempCSVData x on art.art_sif2 = x.ItemNo_
	left join apopust ap on ap.art_sif0 = art.art_sif0
	where ap.art_sif0 is null and art.art_sif0 is not null and isnumeric(replace(x.K5, '%', '')) = 1

	union

	select 
		@Tvrtka as tvr_sif, ap.apo_rb, '1' as apk_sif, '1' as apv_sif, '6' as pag_sif, art.art_sif0, 
		case when isnumeric(replace(x.K6, '%', '')) = 1 then convert(numeric(7, 3), replace(x.K6, '%', '')) else 0 end as apo_popust,
		'' as apo_klk, 'N' as apo_iznos_jmj, @Datum as datum,  @Operater as operater
	from artikl art
	inner join #TempCSVData x on art.art_sif2 = x.ItemNo_
	left join apopust ap on ap.art_sif0 = art.art_sif0
	where ap.art_sif0 is null and art.art_sif0 is not null and isnumeric(replace(x.K6, '%', '')) = 1

	union 

	select 
		@Tvrtka as tvr_sif, ap.apo_rb, '1' as apk_sif, '1' as apv_sif, null as pag_sif,  art.art_sif0, 
		case when isnumeric(replace(x.Loyalty, '%', '')) = 1  then convert(numeric(7, 3), replace(x.Loyalty, '%', '')) else 0 end as apo_popust,
		'%A' as apo_klk, 'N' as apo_iznos_jmj, @Datum as datum, @Operater as operater
	from artikl art
	inner join #TempCSVData x on art.art_sif2 = x.ItemNo_
	left join apopust ap on ap.art_sif0 = art.art_sif0
	where ap.art_sif0 is null and art.art_sif0 is not null and isnumeric(replace(x.Loyalty, '%', '')) = 1
) as a

set @novipopust += @@ROWCOUNT

declare @EVP_GRESKE varchar(max) = ''

set @EVP_GRESKE = 
	'Ažuriranih popusta: ' + cast(@azuriranipopust as varchar(20)) + char(13) + char(10) +
	'Novih popusta: ' + cast(@novipopust as varchar(20)) + char(13) + char(10)

declare @evp_datdo varchar(8) = dbo.SQLDatum(getdate())
declare @evp_vrijemdo varchar(8) = cast(datepart(hh, getdate()) as varchar(2)) + ':' + dbo.vodeci(cast(datepart(mm, getdate()) as varchar(2)), 2, '0') + ':' + dbo.vodeci(cast(datepart(ss, getdate()) as varchar(2)), 2, '0')

set @EVP_GRESKE = 'Prijenos za tvrtku : ' + @Tvrtka + char(13) + char(10) + char(13) + char(10) + 
				  @EVP_GRESKE +  char(13) + char(10) + char(13) + char(10) +  char(13) + char(10) + char(13) + char(10)

insert into evpren (apl_ozn, evp_sif, evp_datod, evp_datdo, evp_vrijemod, evp_vrijemdo, evp_brobrad, evp_brpren, evp_brgres, evp_dug, evp_pot, evp_greske, datum, operater, tvr_sif)
select 
	@rkd_apl_ozn as apl_ozn, 
	@rkd_evp_sif as evp_sif, 
	@evp_datod as evp_datod, 
	@evp_datdo as evp_datdo,
	@evp_vrijemod as evp_vrijemod, 
	@evp_vrijemdo as evp_vrijemdo, 
	count(distinct ItemNo_) as evp_brobrad, 
	count(distinct ItemNo_) as evp_brpren, 
	0 as evp_brgres, 
	0 as evp_dug, 
	0 as evp_pot, 
	@EVP_GRESKE as evp_greske, 
	@Datum as datum, 
	@Operater as operater, 
	@Tvrtka as tvr_sif
from #TempCSVData

select @EVP_GRESKE as greske

    drop table #TempCSVData
end

/*
begin tran
 exec spImportPopustaCSV @Tvrtka = '1', @Operater = 'xxx', @Datum = '20240126', @Putanja = '\\task-sql\SQLData\temp\Grupe_popusta.csv'
rollback
*/