IF EXISTS (SELECT NAME FROM sysobjects WHERE NAME='spImportArtikalaCSV' and TYPE='P')
DROP PROC spImportArtikalaCSV
GO
CREATE PROCEDURE dbo.spImportArtikalaCSV
    @Tvrtka VARCHAR(13),
    @Operater VARCHAR(20),
    @Datum VARCHAR(8),
    @Putanja VARCHAR(200),
	@Pardob VARCHAR(13),
	@Rsk_sif varchar(10),
	@Kkv_sif_new varchar(3)

with encryption as

set nocount on
set ansi_warnings off
set quoted_identifier off

declare @rkd_apl_ozn varchar(3) = 'AD2'
declare @rkd_evp_sif varchar(10)
declare @evp_datod varchar(8) = dbo.SQLDatum(getdate())
declare @evp_vrijemod varchar(8) = cast(datepart(hh, getdate()) as varchar(2)) + ':' + dbo.vodeci(cast(datepart(mm, getdate()) as varchar(2)), 2, '0') + ':' + dbo.vodeci(cast(datepart(ss, getdate()) as varchar(2)), 2, '0')
select @rkd_evp_sif = dbo.Vodeci(isnull(max(cast(evp_sif as int)),0)+1, 10, '_') from evpren where isnumeric(evp_sif) = 1 and apl_ozn = @rkd_apl_ozn

begin
   
    create table #TempCSVData 
	(
		[ItemNo_] VARCHAR(100) COLLATE database_default ,
		[Description] VARCHAR(200) COLLATE database_default,
		[OldDescription] VARCHAR(200) COLLATE database_default,
		[VendorItemNo_] VARCHAR(200) COLLATE database_default,
		[UnitOfMeasure] VARCHAR(200) COLLATE database_default,
		[ProducerItemNo_] VARCHAR(200) COLLATE database_default,
		[BarcodeNo_] VARCHAR(200) COLLATE database_default,
		[ProgramType] VARCHAR(200) COLLATE database_default,
		[DivisionCode] VARCHAR(200) COLLATE database_default,
		[DivisionDescription] VARCHAR(200) COLLATE database_default,
		[ItemCategoryCode] VARCHAR(200) COLLATE database_default,
		[ItemCategoryDescription] VARCHAR(200) COLLATE database_default,
		[ProductGroupCode] VARCHAR(200) COLLATE database_default,
		[ProductGroupDescription] VARCHAR(200) COLLATE database_default,
		[ManufacturerNo] VARCHAR(200) COLLATE database_default,
		[ManufacturerName] VARCHAR(200) COLLATE database_default,
		[RetailPrice] VARCHAR(200) COLLATE database_default,
		[WholeSalePrice] VARCHAR(200) COLLATE database_default,
		[NetoCustomerPrice] VARCHAR(200) COLLATE database_default
	)

    declare @SQL nvarchar(max)
    set @SQL = '
    BULK INSERT #TempCSVData
    FROM ''' + @Putanja + '''
    WITH (
        FIELDTERMINATOR = '';'',
        ROWTERMINATOR = ''\n'',
        FIRSTROW = 2,
		CODEPAGE = 1250
    )'
    exec sp_executesql @SQL

	update #TempCSVData set RetailPrice = replace(RetailPrice, ',', ''), WholeSalePrice = replace(WholeSalePrice, ',', '')

	-----------------------------------------------------------------------Insert u "anomen"------------------------------------------------------------------------------------------------

	declare @anomencount int = 0

	insert into anomen (ano_sif, ano_naz, ano_pri, aktivnost, datum, operater) 
	select 
		a.DivisionCode, 
		a.DivisionDescription,
		case when len(a.DivisionCode) = 9 or a.DivisionCode = '210' then 'D' -- iznimka ako je 210 prijava artikla 'D' 
			 when len(a.DivisionCode) in (3, 6) then 'N' end as ano_pri,
		'D', @Datum, @Tvrtka 
	from ( 
		select distinct dbo.vodeci(left(x.DivisionCode, 3), 3, 0) as DivisionCode, x.DivisionDescription from #TempCSVData x
		union
		select distinct dbo.vodeci(left(x.DivisionCode, 3), 3, 0) + dbo.vodeci(left(x.ItemCategoryCode, 3), 3 ,0) , x.ItemCategoryDescription from #TempCSVData x where x.DivisionCode <> '210'
		union
		select distinct dbo.vodeci(left(x.DivisionCode, 3), 3, 0) + dbo.vodeci(left(x.ItemCategoryCode, 3), 3, 0) + dbo.vodeci(left(ProductGroupCode, 3), 3 , 0), x.ProductGroupDescription from #TempCSVData x where x.DivisionCode <> '210'
	) a 
	where not exists (select ano.ano_sif from anomen ano where ano.ano_sif = a.DivisionCode)
	
	set @anomencount += @@ROWCOUNT
	-----------------------------------------------------------------------Insert u "jmjere"------------------------------------------------------------------------------------------------

	declare @jmjerecount int = 0

	declare @JmjereLen int, @JmjereChar varchar
	select @JmjereLen = dbo.GetFormat('jmjere', 'length'), @JmjereChar = dbo.GetFormat('jmjere', 'char')

	update jmj set jmj.jmj_naz = x.UnitOfMeasure, jmj.jmj_ozn = x.UnitOfMeasure
	from jmjere jmj
	inner join #TempCSVData x on x.UnitOfMeasure = jmj.jmj_ozn
	where jmj.jmj_ozn = x.UnitOfMeasure

	insert into jmjere (jmj_sif, jmj_naz, jmj_ozn, aktivnost, datum, operater)
	select dbo.Vodeci(isnull(a.jmj_sif,0) + row_number() over (order by x.UnitOfMeasure), @JmjereLen, @JmjereChar), 
		x.UnitOfMeasure, left(x.UnitOfMeasure, 4), 'D', @Datum, @Operater 
	from (
		select distinct x.UnitOfMeasure
		from #TempCSVData x
		where not exists (select jmj.jmj_naz from jmjere jmj where jmj.jmj_naz = x.UnitOfMeasure)
	) as x
	left join (
		select max(cast(jmj_sif as integer)) as jmj_sif from jmjere
	) as a on 1=1

	set @jmjerecount += @@ROWCOUNT
	-----------------------------------------------------------------------Insert u "partner"-----------------------------------------------------------------------------------------------

	declare @partnercount int = 0

	declare @greg_par_sif0_autonumber varchar(20) = dbo.GetParam('ŠIFARNICI', 'greg_par_sif0_autonumber', @Operater, @Tvrtka), @greg_par_sif0_ctrlnumber varchar(20)
	if @greg_par_sif0_autonumber = 'TRUE' set @greg_par_sif0_ctrlnumber = dbo.GetParam('ŠIFARNICI', 'greg_par_sif0_ctrlnumber', @Operater, @Tvrtka)

	declare @PartnerLen int = dbo.GetFormat('partner', 'length'), @PartnerChar varchar = dbo.GetFormat('partner', 'char'), @PocSifPart int

	select @PocSifPart = isnull(cast(left(max(par_sif0), @PartnerLen - case when @greg_par_sif0_ctrlnumber = 'TRUE' then 1 else 0 end) as int),0)
	from partner 
	where tvr_sif = @Tvrtka and isnumeric(par_sif0) = 1

	insert into partner (tvr_sif, par_sif0, par_sif1, par_nazs, par_naz0, pav_sif, par_fp, datum_za_tecaj, aktivnost, datum, operater)
	select
			@Tvrtka, dbo.Vodeci(dbo.Modul11(isnull(a.par_sif0,0) + row_number() over (order by x.ManufacturerNo), 1), @PartnerLen, @PartnerChar),
			left(x.ManufacturerNo, 20), isnull(left(x.ManufacturerName, 13),''), isnull(left(x.ManufacturerName, 30),''), '1', '1', '1', 'D', @Datum, @Operater
	from (
	    select distinct x.ManufacturerName, x.ManufacturerNo
	    from #TempCSVData x
	       where 
	        x.ManufacturerNo is not null and x.ManufacturerName is not null
				and not exists (
	            select par.par_naz0 from partner par where par.par_naz0 = x.ManufacturerName
	        )
	        and not exists (
	            select par.par_sif1 from partner par where par.par_sif1 = x.ManufacturerNo
	            and not exists (
	                select par1.par_naz0 from partner par1 where par1.par_naz0 = x.ManufacturerName
	            )
	        ) 
	) as x
	left join (
		select tvr_sif, max(cast(left(par_sif0, len(par_sif0) - 1)as integer)) as par_sif0 from partner par
		group by tvr_sif
	) as a on @Tvrtka = a.tvr_sif

	set @partnercount += @@ROWCOUNT
	-----------------------------------------------------------------------Insert u "artikl"------------------------------------------------------------------------------------------------

	declare @artiklcount int = 0

	declare @ArtiklLen int, @ArtiklChar varchar
	select @ArtiklLen = dbo.GetFormat('artikl', 'length'), @ArtiklChar = dbo.GetFormat('artikl', 'char')

	insert into artikl (tvr_sif, art_sif0, art_sif2, art_bc, ano_sif, art_pardob, art_parpro, art_naz, art_knaz, art_opi, jmj_sif, arv_sif, pog_sif, art_opci, art_negzal, art_boja, 
		art_velic, art_sb, art_jedup, art_odjup, art_tvlup, art_parup, art_sbtip, art_sertip, art_eti, art_klasa, art_oblik, art_zaokr, art_norm_detev, 
		art_tehpost_detev, artduz_jmj, artsir_jmj, artvis_jmj, artdeb_jmj, artmasa_jmj, artgus_jmj, art_dmj, nar_detev, art_sastp_rks, art_norm_detev_poz, aktivnost, datum, operater)
	select distinct 
		@Tvrtka as tvr_sif, dbo.Vodeci(isnull(a.art_sif0, 0) + row_number() over (order by x.ItemNo_), @ArtiklLen, @ArtiklChar) as art_sif0, 
		x.ItemNo_ as art_sif2, left(x.BarcodeNo_ ,13) as art_bc,
		case when left(x.DivisionCode, 3) = '210' THEN '210'
		else dbo.vodeci(left(x.DivisionCode, 3), 3, 0) + dbo.vodeci(left(x.ItemCategoryCode, 3), 3, 0) + dbo.vodeci(left(ProductGroupCode, 3), 3 , 0) end as ano_sif,
		@Pardob as art_pardob,
		par.par_sif0 as art_parpro,
		left(ltrim(rtrim(x.Description)), 40 - len(isnull(ltrim(rtrim(x.VendorItemNo_)), ''))) + ' ' + isnull(ltrim(rtrim(x.VendorItemNo_)), '') as art_naz,
		left(ltrim(rtrim(x.Description)), 20) as art_knaz,  
		(x.Description + ', Broj proizvoðaèa: ' + x.ProducerItemNo_) as art_opi,
		jmj.jmj_sif as jmj_sif,  
		'1' as arv_sif, '1' as pog_sif, 'N' as art_opci, 'N' as art_negzal, 'N' as art_boja, 'N' as art_velic, 'N' as art_sb, 0 as art_jedup, 0 as art_odjup, 0 as art_tvlup, 0 as art_parup, 
		'99' as art_sbtip,  '99' as art_sertip, '1' as art_eti,  'N' as art_klasa,  'N' as art_oblik, '0' as art_zaokr, 'N' as art_norm_detev,  'N' as art_tehpost_detev,
		'1' as artduz_jmj,  '1' as artsir_jmj,  '1' as artvis_jmj,  '1' as artdeb_jmj,  '1' as artmasa_jmj,  '1' as artgus_jmj,  'N' as art_dmj, 
		'1111' as nar_detev, 'N' as art_sastp_rks, 'N' as art_norm_detev_poz,
		'D' as aktivnost, @Datum as datum, @Operater as operater
	from #TempCSVData x
	left join jmjere jmj on x.UnitOfMeasure = jmj.jmj_naz
	left join partner par on x.ManufacturerNo = par.par_sif1
	left join (
		select tvr_sif, max(cast(art_sif0 as numeric(13,0))) as art_sif0 from artikl
		where isnumeric(art_sif0) = 1
		group by tvr_sif 
	) as a on a.tvr_sif = @Tvrtka 
	where not exists (select art.tvr_sif, art.art_sif2 from artikl art where art.tvr_sif = @Tvrtka and art.art_sif2 = x.ItemNo_)  --order by jmj.jmj_sif
	
	set @artiklcount += @@ROWCOUNT
	-----------------------------------------------------------------------Insert u "argrupa" i "ar0grupa"----------------------------------------------------------------------------------

	declare @argrupacount int = 0
	declare @ar0grupacount int = 0

	insert into argrupa (arg_sif, arg_naz, aktivnost, datum, operater)
	select (isnull(a.arg_sif,0) + row_number() over (order by x.ProgramType)),
		x.ProgramType, 'D', @Datum, @Operater 
	from (
		select distinct x.ProgramType
		from #TempCSVData x
		where not exists (select arg.arg_naz from argrupa arg where arg.arg_naz = x.ProgramType)
	) as x
	left join (
		select max(cast(arg_sif as integer)) as arg_sif from argrupa
	) as a on 1=1

	set @argrupacount += @@ROWCOUNT

	insert into ar0grupa (tvr_sif, art_sif0, arg_sif, aktivnost, datum, operater)
	select distinct  @Tvrtka as Tvrtka, art.art_sif0, arg.arg_sif,
		'D' as aktivnost, @Datum as datum, @Operater as operater
	from  #TempCSVData x
	inner join artikl art on art.art_sif2 = x.ItemNo_ and art.tvr_sif = @Tvrtka
	left join argrupa arg on arg.arg_naz = x.ProgramType
	where not exists ( select tvr_sif, art_sif0, arg_sif from ar0grupa ar0 where ar0.tvr_sif = @Tvrtka and ar0.art_sif0 = art.art_sif0 and ar0.arg_sif = arg.arg_sif)

	set @ar0grupacount += @@ROWCOUNT


declare @EVP_GRESKE varchar(max) = ''

set @EVP_GRESKE = 
	'Nomenklaturne grupe: ' + cast(@anomencount as varchar(20)) + char(13) + char(10) +
	'Nove jedinice mjere: ' + cast(@jmjerecount as varchar (20)) + char(13) + char(10) +
	'Novi partneri: ' + cast(@partnercount as varchar (20)) + char(13) + char(10) +
	'Novi artikli: ' + cast(@artiklcount as varchar (20)) + char(13) + char(10) +
	'Nove grupe artikala: ' + cast(@argrupacount as varchar (20)) + char(13) + char(10) +
	'Novi artikli u grupama artikala: ' + cast(@ar0grupacount as varchar (20))

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
 exec spImportArtikalaCSV @Tvrtka = '1', @Operater = 'xxx', @Datum = '20240115', @Putanja = '\\task-sql\SQLData\temp\ItemData.txt', @Pardob = '000329', @Rsk_sif = '1', @kkv_sif_new = '999' 
rollback
*/