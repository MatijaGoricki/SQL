IF EXISTS (SELECT NAME FROM sysobjects WHERE NAME='spImportCrossListeCSV' and TYPE='P')
DROP PROC spImportCrossListeCSV
GO
CREATE PROCEDURE dbo.spImportCrossListeCSV
    @Tvrtka VARCHAR(13),
    @Operater VARCHAR(20),
    @Datum VARCHAR(8),
    @Putanja VARCHAR(200),
	@Pardob VARCHAR(13)
with encryption as

declare @rkd_apl_ozn varchar(3) = 'AD2'
declare @rkd_evp_sif varchar(10)
declare @evp_datod varchar(8) = dbo.SQLDatum(getdate())
declare @evp_vrijemod varchar(8) = cast(datepart(hh, getdate()) as varchar(2)) + ':' + dbo.vodeci(cast(datepart(mm, getdate()) as varchar(2)), 2, '0') + ':' + dbo.vodeci(cast(datepart(ss, getdate()) as varchar(2)), 2, '0')
select @rkd_evp_sif = dbo.Vodeci(isnull(max(cast(evp_sif as int)),0)+1, 10, '_') from evpren where isnumeric(evp_sif) = 1 and apl_ozn = @rkd_apl_ozn

begin
    delete from artzam
   
    create table #TempCSVData (
        ItemNo_ varchar(100) collate database_default,
        SubstituteNo_ varchar(100) collate database_default
    )

    declare @SQL nvarchar(max)
    set @SQL = '
    BULK INSERT #TempCSVData
    FROM ''' + @Putanja + '''
    WITH (
        FIELDTERMINATOR = '';'',
        ROWTERMINATOR = ''\n'',
        FIRSTROW = 2
    )'
    EXEC sp_executesql @SQL

	declare @zamjenskiart int = 0

	insert into artzam (tvr_sif, art_sif0, rbr_artzam, art_sifz, aktivnost, datum, operater)
	select
		@Tvrtka as Tvrtka,
		a_item.art_sif0 as Šifra_artikla,
		ROW_NUMBER() OVER (PARTITION BY a_item.art_sif0 ORDER BY a_substitute.art_sif0) as Redni_broj,
		a_substitute.art_sif0 as Zamjenska_šifra,
		'D' as Aktivnost,
		@Datum as Datum,
		@Operater as Operater
	from #TempCSVData t
	inner join artikl a_item on a_item.art_sif2 = t.ItemNo_
		and a_item.art_pardob = @Pardob  
		AND a_item.tvr_sif = @Tvrtka
	inner join artikl a_substitute on a_substitute.art_sif2 = t.SubstituteNo_ 
		and a_substitute.art_pardob = @Pardob  
		and a_substitute.tvr_sif = @Tvrtka


set @zamjenskiart += @@ROWCOUNT
declare @EVP_GRESKE varchar(max) = ''

set @EVP_GRESKE = 
	'Zamjenski artikli: ' + cast(@zamjenskiart as varchar(20)) + char(13) + char(10) 


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

-- exec spImportCrossListeCSV @Tvrtka = '1', @Operater = '999', @Datum = '20240109', @Putanja = '\\task-sql\SQLData\temp\ItemSubstitute.txt', @Pardob = '000329'