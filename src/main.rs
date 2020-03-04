use kvdb_rocksdb::*;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::fs;
use std::io::{Error as IoError, Read as _, Write as _};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Cli {
    /// The path to the db folder.
    /// Example: `~/.local/share/io.parity.ethereum/chains/ethereum/db/906a34e69aec8c0d/overlayrecent`.
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

const CURRENT_VERSION: u32 = 14;
const CURRENT_COLUMNS: u32 = 9;
const DOWNGRADE_VERSION: u32 = 13;
const VERSION_FILE_NAME: &str = "db_version";

#[derive(Debug)]
pub enum Error {
    /// Returned when current version cannot be read or guessed.
    UnknownDatabaseVersion,
    /// Migration was completed succesfully,
    /// but there was a problem with io.
    Io(IoError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        let out = match *self {
            Error::UnknownDatabaseVersion => "Current database version cannot be read".into(),
            Error::Io(ref err) => format!("Unexpected io error on DB migration: {}.", err),
        };

        write!(f, "{}", out)
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Self {
        Error::Io(err)
    }
}

fn version_file_path(mut path: PathBuf) -> PathBuf {
    path.push(VERSION_FILE_NAME);
    path
}

fn update_version(path: PathBuf) -> Result<(), Error> {
    fs::create_dir_all(&path)?;
    let mut file = fs::File::create(version_file_path(path))?;
    file.write_all(format!("{}", DOWNGRADE_VERSION).as_bytes())?;
    Ok(())
}

fn current_version(path: PathBuf) -> Result<u32, Error> {
    match fs::File::open(version_file_path(path)) {
        Err(_) => Err(Error::UnknownDatabaseVersion),
        Ok(mut file) => {
            let mut s = String::new();
            file.read_to_string(&mut s)
                .map_err(|_| Error::UnknownDatabaseVersion)?;
            u32::from_str_radix(&s, 10).map_err(|_| Error::UnknownDatabaseVersion)
        }
    }
}

fn database_path(mut path: PathBuf) -> PathBuf {
    path.push("db");
    path
}

fn downgrade_database(db_path: PathBuf) -> Result<(), Error> {
    // check if a migration is needed
    let current_version = current_version(db_path.clone())?;
    if current_version == DOWNGRADE_VERSION {
        println!("No migration is needed.");
        return Ok(());
    }

    if current_version != CURRENT_VERSION {
        return Err(Error::UnknownDatabaseVersion);
    }

    let db_config = DatabaseConfig::with_columns(CURRENT_COLUMNS);
    let path = database_path(db_path.clone());

    println!("[1/4] Opening the database...");
    let db = Database::open(&db_config, &path.to_string_lossy())?;
    println!("[2/4] Removing a column...");
    db.remove_last_column()?;
    println!("[3/4] Updating a version...");
    update_version(db_path)?;
    println!("[4/4] Migration completed.");

    Ok(())
}

fn main() -> Result<(), Error> {
    eprintln!(
        r#"
+====================================================================================+
| [!] WARNING! USE IT AT YOUR OWN RISK!                                           [!]|
=====================================================================================+
    "#
    );

    let args = Cli::from_args();
    downgrade_database(args.path)
}
