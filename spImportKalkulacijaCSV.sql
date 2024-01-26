IF EXISTS (SELECT NAME FROM sysobjects WHERE NAME='spImportKalkulacijaCSV' and TYPE='P')
DROP PROC spImportKalkulacijaCSV
GO
CREATE PROCEDURE dbo.spImportKalkulacijaCSV
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

	-----------------------------------------------------------------------Kalkulacije------------------------------------------------------------------------------------------------------

	declare @greg_valuta varchar(3), @greg_tecvrsta varchar(10)
	select @greg_valuta = dbo.GetParam('ALL', 'greg_valuta', @Operater, @Tvrtka), @greg_tecvrsta = dbo.GetParam('ROBNO', 'greg_tecvrsta', @Operater, @Tvrtka)

	declare @greg_biranje_kalkulacije varchar(10) = dbo.GetParam('ROBNO', 'greg_biranje_kalkulacije', default, default)

	declare @kTvr_sif varchar(13), @kItemNo varchar(15), @kRetailPrice numeric(18, 6), @kWholeSalePrice numeric(18, 6), @kArt_sif0 varchar(13)
	declare @Kkv_sif varchar(3), @Kal_sif varchar(10), @Kal_sif_new varchar(10)
	declare @mpc numeric(18, 6)

	declare @nova_kalkulacija int
	declare @novekalkulacijecount int = 0
	declare @kopiranekalkulacijecount int = 0


	declare csv_cijene cursor for
		select ItemNo_, RetailPrice, WholeSalePrice
		from #TempCSVData
	open csv_cijene
	fetch next from csv_cijene into @kItemNo, @kRetailPrice, @kWholeSalePrice
	while @@fetch_status = 0
	begin

		select @kArt_sif0 = art_sif0 from artikl where art_sif2 = @kItemNo
		
		select @Kkv_sif = null, @Kal_sif = null, @Kal_sif_new = null, @nova_kalkulacija = 0

		exec spBiranje_kalkulacije @Tvrtka, @kArt_sif0, null, @Datum, null, null, @Rsk_sif, @greg_biranje_kalkulacije, @Kkv_sif output, @Kal_sif output

		select @mpc = dbo.IzvadiVEK(@Tvrtka, @kArt_sif0, @Kkv_sif, @Kal_sif, 'MPC', 1)
	
		--ako je mp cijena razlièita, iskopirati kalkulaciju u novi broj, promijeniti nabavni dio a prodajni mora ostati isti kao i kod prethodne kalkulacije
		if @mpc <> @kRetailPrice begin

		--kopiranje u novi broj
			declare @KalkulLen int, @KalkulChar varchar
			select @KalkulLen = dbo.GetFormat('kalkul', 'length'), @KalkulChar = dbo.GetFormat('kalkul', 'char')

			if @kkv_sif is null set @kkv_sif = @Kkv_sif_new 

			-- kopiranje kalkulacije
			if @Kkv_sif is not null and @Kal_sif is not null begin

				set @nova_kalkulacija = 1

				select @Kal_sif_new = dbo.Vodeci(isnull(cast(max(kal_sif) as numeric(18,0)),0)+1, @KalkulLen, @KalkulChar) from kazagl
				where tvr_sif = @Tvrtka and art_sif0 = @kArt_sif0 and kkv_sif = @Kkv_sif	

				insert into kazagl (tvr_sif, art_sif0, kkv_sif, kal_sif, kaz_dat, kaz_opis, aktivnost, datum, operater, kaz_val_sif, kaz_dodatuma)
				select tvr_sif, art_sif0, kkv_sif, @Kal_sif_new, @Datum, 'Tokiæ', aktivnost, @Datum, @Operater, kaz_val_sif, kaz_dodatuma 
				from kazagl
				where tvr_sif = @Tvrtka and art_sif0 = @kArt_sif0 and kkv_sif = @Kkv_sif and kal_sif = @Kal_sif
	
				insert into kalkul (tvr_sif, art_sif0, kkv_sif, kal_sif, kae_sif, kal_pos, kal_aps)
				select tvr_sif, art_sif0, kkv_sif, @Kal_sif_new, kae_sif, kal_pos, kal_aps
				from kalkul
				where tvr_sif = @Tvrtka and art_sif0 = @kArt_sif0 and kkv_sif = @Kkv_sif and kal_sif = @Kal_sif
				
				set @kopiranekalkulacijecount += 1

			end	else begin -- kreiranje nove kalkulacije

				set @nova_kalkulacija = 2
			
				select @Kal_sif_new = dbo.Vodeci(isnull(cast(max(kal_sif) as numeric(18,0)),0)+1, @KalkulLen, @KalkulChar) from kazagl
				where tvr_sif = @Tvrtka and art_sif0 = @kArt_sif0 and kkv_sif = @Kkv_sif

				insert into kazagl (tvr_sif, art_sif0, kkv_sif, kal_sif, kaz_dat, kaz_opis, aktivnost, datum, operater)
				select @Tvrtka, @kArt_sif0, @Kkv_sif, @Kal_sif_new, @Datum, 'Tokiæ' , 'D', @Datum, @Operater
	
				insert into kalkul (tvr_sif, art_sif0, kkv_sif, kal_sif, kae_sif, kal_pos, kal_aps)
				select @Tvrtka, @kArt_sif0, @Kkv_sif, @Kal_sif_new, kae_sif, 0, case when  kae_sif = 'N01'  then @kWholeSalePrice else 0 end --cijena dobavljaca
				from ka0eleme 
				where kkv_sif = @Kkv_sif

				exec spPreracunKalkulacije @Tvrtka, @kArt_sif0, @Kkv_sif, @Kal_sif_new, 0, @Datum, '1;1;1;1;1;0', null, null, @Operater, @Datum, @Operater 

				set @novekalkulacijecount += 1
				
			end	
				update kalkul set kal_aps = @kRetailPrice where tvr_sif = @Tvrtka and art_sif0 = @kArt_sif0 and kkv_sif = @Kkv_sif and kal_sif = @Kal_sif_new and kae_sif = 'C02'
				exec spPreracunKalkulacije @Tvrtka, @kArt_sif0, @Kkv_sif, @Kal_sif_new, 0, @Datum, '0;0;0;0;0;1', null, null, @Operater, @Datum, @Operater 
		end

		if @kal_sif is null set @kkv_sif = null

		fetch next from csv_cijene into @kItemNo, @kRetailPrice, @kWholeSalePrice
	end
	close csv_cijene
	deallocate csv_cijene


declare @EVP_GRESKE varchar(max) = ''

set @EVP_GRESKE = 
	'Kopirane kalkulacije: ' + cast(@kopiranekalkulacijecount as varchar (20)) + char(13) + char(10) +
	'Novo nastale kalkulacije: ' + cast(@novekalkulacijecount as varchar (20))

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
 exec spImportKalkulacijaCSV @Tvrtka = '1', @Operater = 'xxx', @Datum = '20240115', @Putanja = '\\task-sql\SQLData\temp\ItemData.txt', @Pardob = '000329', @Rsk_sif = '1', @kkv_sif_new = '999' 
rollback
*/