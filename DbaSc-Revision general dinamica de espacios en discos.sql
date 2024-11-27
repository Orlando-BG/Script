--======================================================================================================

USE [master]
GO

SET NOCOUNT ON

declare @name as varchar(max), 
	@db as varchar(max),
	@RutaArchivo as varchar(max),
	@RecoveryModel nvarchar(60),
	@Growth int, @max_size int,
	@sql as varchar(max)

--======================================================================================================

IF object_id('tempdb.dbo.#datafiles', 'U') IS NOT NULL
	DROP TABLE #datafiles

CREATE TABLE #datafiles
(	[NombreDb] [varchar](MAX) NOT NULL,
	[FileGroup] [varchar](MAX) NOT NULL,	
	[NombreArchivo] [sysname] NOT NULL,	
	[RecoveryModel] [nvarchar] (60), 
	[is_percent_growth] [bit],
	[Growth] [int], [max_size] [int],
	[SizeArchivo] [int] NOT NULL, [EspacioOcupado] [int] NULL, [EspacioLiberar] [int] NULL,
	[Tipo] [varchar](MAX) NOT NULL,
	[RutaArchivo] [varchar](MAX) NOT NULL
)

declare CURSOR_001 cursor for
SELECT	a.name as filedb, b.name as db, physical_name as RutaArchivo, 
	b.recovery_model_desc, a.growth, a.max_size
FROM sys.master_files as a 
inner join sys.databases as b 
on a.database_id = b.database_id and b.state_desc = 'ONLINE'
		
open CURSOR_001
fetch next from CURSOR_001
into @name, @db, @RutaArchivo, @RecoveryModel, @Growth, @max_size
while @@fetch_status = 0
begin

	set @sql = 
	'
	use [' + @db + '];
	insert into #datafiles
	SELECT	''' + @db + ''' as db, ISNULL(fg.name, ''Not Applicable'') AS [FileGroup], df.name as NombreArchivo, ''' 
				+ @RecoveryModel + ''' as RecoveryModel, is_percent_growth, '
				+ cast(@Growth as varchar(10)) +' as Growth,'
				+ cast(@max_size as varchar(20)) +' as max_size,
			isnull((isnull(size,0)/128.0 - cast(isnull(size,0)/128.0 - CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS int)/128.0 as int) +
			cast(isnull(size,0)/128.0 - CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS int)/128.0 as int)),0) TotalArchivo,
			isnull(isnull(size,0)/128.0 - cast(isnull(size,0)/128.0 - CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS int)/128.0 as int),0) as EspacioOcupado,
			isnull(cast(isnull(size,0)/128.0 - CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS int)/128.0 as int),0) EspacioShrink,
			df.type_desc as Tipo, 
			''' + @RutaArchivo + ''' as RutaArchivo
	FROM sys.database_files df
		LEFT OUTER JOIN sys.filegroups fg on df.data_space_id = fg.data_space_id
	WHERE df.name = ''' + @name + ''' --and type_desc = ''LOG'''

	exec (@sql);
	
fetch next from CURSOR_001
into @name, @db, @RutaArchivo, @RecoveryModel, @Growth, @max_size
end

close CURSOR_001
deallocate CURSOR_001

--======================================================================================================

IF object_id('tempdb.dbo.#fixeddrives', 'U') IS NOT NULL
	DROP TABLE #fixeddrives

CREATE TABLE #fixeddrives
(
	DISCO nvarchar(10), 
	MB_LIBRES int
)

INSERT INTO #fixeddrives
exec xp_fixeddrives

--======================================================================================================

IF object_id('tempdb.dbo.#espacio', 'U') IS NOT NULL
	DROP TABLE #espacio

select a.DISCO as disco, b.tama�o_gb, 
	cast(a.MB_LIBRES/1024.00 as numeric(18,2)) as libre_gb, 
	cast(((a.MB_LIBRES/1024.00)/b.tama�o_gb) * 100 as numeric(18,2)) as [libre %]
	into #espacio
from #fixeddrives a
	left outer join SQLAdmin.dba.disco b on a.DISCO = b.disco

--======================================================================================================

IF object_id('tempdb.dbo.#sql', 'U') IS NOT NULL
	DROP TABLE #sql

;with t1 as
(
select substring(RutaArchivo, 1,1) as disco,
	Tipo,
	cast(SizeArchivo / 1024.00 as numeric(18,2)) as size_gb
from #datafiles
), t2 as
(
select substring(RutaArchivo, 1,1) as disco,
	Tipo,
	cast(EspacioLiberar / 1024.00 as numeric(18,2)) as aliberar_gb	
from #datafiles
), pvt_size as
(
select *
from t1
pivot
( sum(size_gb)
for Tipo 
in ([ROWS],[LOG])
) pvt1
), pvt_liberar as
(
select *
from t2
pivot
( sum(aliberar_gb)
for Tipo 
in ([ROWS],[LOG])
) pvt2
)
SELECT a.disco, 
	[size (data_gb)] = a.[ROWS], 
	[size (log_gb)] = a.[LOG],
	[aliberar (data_gb)] = b.[ROWS], 
	[aliberar (log_gb)] = b.[LOG]
	into #sql
FROM pvt_size a
	INNER JOIN pvt_liberar b on a.disco = b.disco

--======================================================================================================

IF object_id('tempdb.dbo.#discos', 'U') IS NOT NULL
	DROP TABLE #discos

select a.*, b.[size (data_gb)], b.[size (log_gb)], b.[aliberar (data_gb)], b.[aliberar (log_gb)]
	into #discos
from #espacio a
	left outer join #sql b on a.disco = b.disco

--======================================================================================================

select *
from #discos
where libre_gb < 30 and (libre_gb / tama�o_gb) < 0.2
order by libre_gb

--======================================================================================================

select *
from #discos

--======================================================================================================
/*
select *
from #datafiles
where substring(RutaArchivo, 1 , 1) = 'Q'
*/
--======================================================================================================

/*
IF object_id('tempdb.dbo.#sql', 'U') IS NOT NULL
	DROP TABLE #sql

select disco, isnull([ROWS], 0) as data_gb, isnull([LOG], 0) as log_gb 
into #sql
from (
select substring(RutaArchivo, 1,1) as disco, Tipo,
	cast(sum(SizeArchivo) / 1024.00 as numeric(18,2)) as tama�o_gb--,
	--cast(sum(EspacioLiberar) / 1024.00 as numeric(18,2)) as a_liberar_gb
from #datafiles
group by substring(RutaArchivo, 1,1), Tipo
--order by disco, tama�o_gb desc
) as t
pivot
(
max(tama�o_gb) for Tipo in([ROWS], [LOG]) 
) as pvt1

--======================================================================================================

IF object_id('tempdb.dbo.#discos', 'U') IS NOT NULL
	DROP TABLE #discos

select a.*, 
	isnull(b.data_gb, 0 ) as data_gb, isnull(b.log_gb, 0) as log_gb,
	case when a.[tama�o_gb] is not null then a.tama�o_gb - (a.libre_gb + isnull(b.data_gb, 0) + isnull(b.log_gb, 0)) end as no_sql_gb
	--a.tama�o_gb - (a.libre_gb + b.data_gb + b.log_gb) as no_sql_gb
	--case when b.data_gb is null and b.log_gb is null and a.[tama�o_gb] is not null and a.libre_gb is not null then a.tama�o_gb - a.libre_gb else 0 end no_sql_gb
	into #discos
from #espacio a
	left outer join #sql b on a.disco = b.disco

--======================================================================================================

select *, cast(libre_gb / tama�o_gb as numeric(18,2)) as [% libre]
from #discos
where libre_gb < 30 and (libre_gb / tama�o_gb) < 0.2
order by libre_gb
*/
--======================================================================================================
/*
select *
from #datafiles d
where substring(d.RutaArchivo, 1, 1) = 'E'

--======================================================================================================

select dk.DISCO as Disco, df.Tipo, df.NombreDb, CAST(CAST(SUM(df.SizeArchivo) AS FLOAT)/1024 AS NUMERIC(18,2)) SizeArchivo_GB
	, CAST(CAST(SUM(df.EspacioOcupado) AS FLOAT)/1024 AS NUMERIC(18,2)) Ocupado_GB
	, CAST(CAST(SUM(df.EspacioLiberar) AS FLOAT)/1024 AS NUMERIC(18,2)) EspacioLiberar_GB
	, CAST(CAST(dk.MB_LIBRES AS FLOAT)/1024 AS NUMERIC(18,2)) LIBRES_GB
from #fixeddrives dk
	left outer join #datafiles df on dk.DISCO = substring(df.RutaArchivo,1,1)
GROUP BY dk.DISCO, df.NombreDb, df.Tipo, dk.MB_LIBRES
ORDER BY dk.DISCO, df.Tipo desc, SizeArchivo_GB desc

--======================================================================================================

DECLARE @logspace table
(	
	Database_Name varchar(150),
	Log_Size float,
	Log_Space float,
	[Status] varchar(100)
)

DECLARE @datafiles table 
(	NombreDb varchar(MAX),	
	NombreFileGroup varchar(MAX),
	[FileGroup] varchar(MAX),
	NombreArchivo sysname,	
	RecoveryModel nvarchar(60), 
	AutoGrowth char(2), 
	Growth varchar(512),
	Max_size varchar(100),
	SizeArchivo int, EspacioOcupado int, EspacioLiberar int, 
	Tipo varchar(MAX), RutaArchivo varchar(MAX), Disco char(1)
)

--======================================================================================================

INSERT INTO @logspace
EXEC('DBCC sqlperf(logspace) WITH NO_INFOMSGS')

INSERT INTO @datafiles
select NombreDb, [FileGroup], 
	case when [FileGroup] like 'TB_%' then 'TABLES' else [FileGroup] end as FG,
	NombreArchivo,
	RecoveryModel, 
	case Growth when 0 then 'NO' else 'SI' end AutoGrowth,
	case when is_percent_growth = 0 then cast(Growth/128 as varchar(256)) + ' MB' when is_percent_growth = 1 then cast(Growth as varchar(256)) + ' %' end Growth,
	case max_size when 0 then 'No growth is allowed' when -1 then 'Unlimited' else 'Limited to ' + convert(varchar, max_size/128) end Max_size,
	SizeArchivo, EspacioOcupado, EspacioLiberar,
	Tipo, RutaArchivo, UPPER(Substring(RutaArchivo, 0, 2)) Disco 
from #datafiles



select * from @datafiles

--======================================================================================================

select *
from 
(	select df.Disco, df.Tipo, df.NombreDb, CAST(CAST(SUM(df.SizeArchivo) AS FLOAT)/1024 AS NUMERIC(18,2)) SizeArchivo_GB
		, CAST(CAST(SUM(df.EspacioOcupado) AS FLOAT)/1024 AS NUMERIC(18,2)) Ocupado_GB
		, CAST(CAST(SUM(df.EspacioLiberar) AS FLOAT)/1024 AS NUMERIC(18,2)) EspacioLiberar_GB
		, CAST(CAST(dk.MB_LIBRES AS FLOAT)/1024 AS NUMERIC(18,2)) LIBRES_GB
	from @datafiles df
		left outer join #fixeddrives dk on df.Disco = dk.DISCO
	GROUP BY df.Disco, df.NombreDb, df.Tipo, dk.MB_LIBRES
)a
ORDER BY a.Disco, a.Tipo, a.NombreDb

select * from @logspace

----======================================================================================================

select *
from 
(	select df.Disco, df.Tipo, df.NombreDb, df.[FileGroup], df.AutoGrowth, count(df.NombreArchivo) as cantidad, df.RecoveryModel
		, CAST(CAST(SUM(df.EspacioOcupado) AS FLOAT)/1024 AS NUMERIC(18,2)) Ocupado_GB
		, CAST(CAST(SUM(df.EspacioLiberar) AS FLOAT)/1024 AS NUMERIC(18,2)) EspacioLiberar_GB
		, CAST(CAST(MAX(dk.MB_LIBRES) AS FLOAT)/1024 AS NUMERIC(18,2)) LIBRES_GB
	from @datafiles df
		left outer join #fixeddrives dk on df.Disco = dk.DISCO
	group by df.Disco, df.Tipo, df.NombreDb, df.[FileGroup], df.AutoGrowth, df.RecoveryModel
)a
PIVOT(count(AutoGrowth) FOR AutoGrowth IN ([SI],[NO])) AS PvtAutoGrowth
--where EspacioLiberar_GB > 0.1
--where Tipo = 'LOG'
--where Tipo = 'ROWS'
--where [SI] > 0
--where [FileGroup] = 'HISTORICO_CASHMANAGEMENT_DATA2'
ORDER BY Tipo, NombreDb, [FileGroup]

----======================================================================================================

declare @reservado int
set @reservado = 0

select df.NombreDb, df.[FileGroup], df.NombreFileGroup, df.NombreArchivo, df.RecoveryModel	
	, df.AutoGrowth
	, df.Growth	
	, df.Max_size, df.SizeArchivo, df.EspacioOcupado
	--, @reservado reservado, df.EspacioOcupado + @reservado suma
	, df.EspacioLiberar	, dk.MB_LIBRES, round(l.Log_Space,2) [Log Space Used (%)]
	, df.Tipo, DF.Disco
	, reverse(substring(reverse(df.RutaArchivo), 1, charindex('\',reverse(df.RutaArchivo)) -1)) Archivo
	, df.RutaArchivo	
	--, 'USE [' + NombreDb + ']; DBCC SHRINKFILE (N''' + NombreArchivo + ''', ' + CASE Tipo WHEN 'LOG' THEN '0' ELSE cast(EspacioOcupado as varchar(10)) END + ');' SQLStatement
	, 'USE [' + NombreDb + ']; DBCC SHRINKFILE (N''' + NombreArchivo + ''', ' + 
		--CASE Tipo WHEN 'LOG' THEN '0' ELSE 
		CASE WHEN EspacioOcupado + @reservado > df.SizeArchivo THEN NULL /*cast(EspacioOcupado as varchar(10))*/ 
		ELSE cast(EspacioOcupado + @reservado as varchar(10)) 
		--END	
	--cast(EspacioOcupado as varchar(10)) 
	END + ');' SQLStatement
	, CASE WHEN df.Tipo = 'LOG' AND df.RecoveryModel = 'FULL' THEN '--USE [' + NombreDb + ']; BACKUP LOG [' + NombreDb + '] TO DISK= ''NUL:''; CHECKPOINT;' END SQLStatementBackupLog
from @datafiles df
	left outer join @logspace l on df.NombreDb = l.Database_Name
	left outer join #fixeddrives dk on df.Disco = dk.DISCO
--WHERE df.EspacioLiberar > 50
	--and df.Disco = 'E'
--where df.NombreFileGroup = 'HISTORICO_CASHMANAGEMENT_DATA2'
--where df.AutoGrowth = 'SI'
--where df.Tipo = 'LOG'
--where Tipo = 'ROWS'
order by df.Tipo, EspacioLiberar desc

*/
