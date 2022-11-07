use indicatif::ProgressBar;
use postgres::types::ToSql;
use quick_xml::events::Event;
use std::collections::BTreeMap;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_releases, DbOpt, SqlSerialization};
use crate::parser::Parser;

#[derive(Clone, Debug)]
pub struct Track {
    position: String,
    title: String,
    duration: String,
    release_id: i32,
}

#[derive(Clone, Debug)]
pub struct Format {
    name: String,
    qty: String,
    text: String,
    release_id: i32,
    // TODO Descriptions
}

impl Format {
    fn new(release_id: i32, name: String, qty: String, text: String) -> Format {
        Format { name, qty, text, release_id }
    }    
}

impl SqlSerialization for Format {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.release_id,
            &self.name,
            &self.qty,
            &self.text,
        ];
        row
    }
}


impl Track {
    fn new(release_id: i32) -> Track {
        Track {
            release_id,
            position: String::new(),
            title: String::new(),
            duration: String::new(),
        }
    }
}

impl SqlSerialization for Track {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.release_id,
            &self.title,
            &self.position,
            &self.duration,
        ];
        row
    }
}

#[derive(Clone, Debug)]
pub struct Release {
    pub id: i32,
    pub status: String,
    pub title: String,
    pub country: String,
    pub released: String,
    pub notes: String,
    pub genres: Vec<String>,
    pub styles: Vec<String>,
    pub master_id: i32,
    pub data_quality: String,
}


impl SqlSerialization for Release {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.id,
            &self.status,
            &self.title,
            &self.country,
            &self.released,
            &self.notes,
            &self.genres,
            &self.styles,
            &self.master_id,
            &self.data_quality,
        ];
        row
    }
}

#[derive(Clone, Debug)]
pub struct ReleaseLabel {
    pub release_id: i32,
    pub label: String,
    pub catno: String,
    pub label_id: i32,
}

impl SqlSerialization for ReleaseLabel {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> =
            vec![&self.release_id, &self.label, &self.catno, &self.label_id];
        row
    }
}

#[derive(Clone, Debug)]
pub struct ReleaseVideo {
    pub release_id: i32,
    pub duration: i32,
    pub src: String,
    pub title: String,
}

impl SqlSerialization for ReleaseVideo {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> =
            vec![&self.release_id, &self.duration, &self.src, &self.title];
        row
    }
}

impl Release {
    pub fn new(id: i32) -> Self {
        Release {
            id,
            status: String::new(),
            title: String::new(),
            country: String::new(),
            released: String::new(),
            notes: String::new(),
            genres: Vec::new(),
            styles: Vec::new(),
            master_id: 0,
            data_quality: String::new(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserReadState {
    // release
    Release,
    Title,
    Country,
    Released,
    Notes,
    Genres,
    Genre,
    Styles,
    Style,
    MasterId,
    DataQuality,
    // release_label
    Labels,
    // release_video
    Videos,

    TrackList,
    Track,
    TrackPosition,
    TrackTitle,
    TrackDuration,

    Images,
    Artists,
    ExtraArtists,
    Formats,
    Format,
    Identifiers,
    Companies,
}

pub struct ReleasesParser<'a> {
    state: ParserReadState,
    releases: HashMap<i32, Release>,
    current_release: Release,
    current_id: i32,
    release_labels: HashMap<i32, ReleaseLabel>,
    current_video_id: i32,
    release_videos: HashMap<i32, ReleaseVideo>,
    current_track_id: i32,
    tracks: BTreeMap<i32, Track>,
    current_format_id: i32,
    formats: BTreeMap<i32, Format>,
    pb: ProgressBar,
    db_opts: &'a DbOpt,
}

impl<'a> ReleasesParser<'a> {
    pub fn new(db_opts: &'a DbOpt) -> Self {
        ReleasesParser {
            state: ParserReadState::Release,
            releases: HashMap::new(),
            current_release: Release::new(0),
            current_id: 0,
            release_labels: HashMap::new(),
            current_video_id: 0,
            release_videos: HashMap::new(),
            current_track_id: 0,
            tracks: BTreeMap::new(),
            current_format_id: 0,
            formats: BTreeMap::new(),
            pb: ProgressBar::new(14976967), // https://api.discogs.com/
            db_opts,
        }
    }
}

impl<'a> Parser<'a> for ReleasesParser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self {
        ReleasesParser {
            state: ParserReadState::Release,
            releases: HashMap::new(),
            current_release: Release::new(0),
            current_id: 0,
            release_labels: HashMap::new(),
            current_video_id: 0,
            release_videos: HashMap::new(),
            current_track_id: 0,
            tracks: BTreeMap::new(),
            current_format_id: 0,
            formats: BTreeMap::new(),
            pb: ProgressBar::new(14976967), // https://api.discogs.com/
            db_opts,
        }
    }

    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserReadState::Release => {
                match ev {
                    Event::Start(e) if e.local_name() == b"release" => {
                        self.current_id = str::parse(str::from_utf8(
                            &e.attributes().next().unwrap()?.unescaped_value()?,
                        )?)?;
                        self.current_release = Release::new(self.current_id);
                        self.current_release.status = str::parse(str::from_utf8(
                            &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                        )?)?;
                        ParserReadState::Release
                    }

                    Event::Start(e) => match e.local_name() {
                        b"title" => ParserReadState::Title,
                        b"country" => ParserReadState::Country,
                        b"released" => ParserReadState::Released,
                        b"notes" => ParserReadState::Notes,
                        b"genres" => ParserReadState::Genres,
                        b"styles" => ParserReadState::Styles,
                        b"master_id" => ParserReadState::MasterId,
                        b"data_quality" => ParserReadState::DataQuality,
                        b"labels" => ParserReadState::Labels,
                        b"videos" => ParserReadState::Videos,
                        b"tracklist" => ParserReadState::TrackList,
                        b"images" => ParserReadState::Images,
                        b"artists" => ParserReadState::Artists,
                        b"extraartists" => ParserReadState::ExtraArtists,
                        b"formats" => ParserReadState::Formats,
                        b"identifiers" => ParserReadState::Identifiers,
                        b"companies" => ParserReadState::Companies,
                        _ => ParserReadState::Release,
                    },

                    Event::End(e) if e.local_name() == b"release" => {
                        self.releases
                            .entry(self.current_id)
                            .or_insert(self.current_release.clone());
                        if self.releases.len() >= self.db_opts.batch_size {
                            // write to db every 1000 records and clean the hashmaps
                            // use drain? https://doc.rust-lang.org/std/collections/struct.HashMap.html#examples-13
                            write_releases(
                                self.db_opts,
                                &self.releases,
                                &self.release_labels,
                                &self.release_videos,
                                &self.tracks,
                                &self.formats,
                            )?;
                            self.releases = HashMap::new();
                            self.release_labels = HashMap::new();
                            self.release_videos = HashMap::new();
                            self.tracks = BTreeMap::new();
                            self.formats = BTreeMap::new();
                        }
                        self.pb.inc(1);
                        ParserReadState::Release
                    }

                    Event::End(e) if e.local_name() == b"releases" => {
                        // write to db remainder of releases
                        write_releases(
                            self.db_opts,
                            &self.releases,
                            &self.release_labels,
                            &self.release_videos,
                            &self.tracks,
                            &self.formats,
                        )?;
                        ParserReadState::Release
                    }

                    _ => ParserReadState::Release,
                }
            }

            // Just eat this for now
            ParserReadState::TrackList => match ev {
                Event::Start(e) => match e.local_name() {
                    b"track" => ParserReadState::Track,
                    _ => ParserReadState::TrackList,
                },

                Event::End(e) if e.local_name() == b"tracklist" => ParserReadState::Release,

                _ => ParserReadState::TrackList,
            },

            ParserReadState::Track => match ev {
                Event::Start(e) => match e.local_name() {
                    b"title" => ParserReadState::TrackTitle,
                    b"position" => ParserReadState::TrackPosition,
                    b"duration" => ParserReadState::TrackDuration,
                    _ => ParserReadState::Track,
                },

                Event::End(e) if e.local_name() == b"track" => {
                    self.current_track_id += 1;
                    ParserReadState::TrackList
                }

                _ => ParserReadState::Track,
            },

            ParserReadState::TrackTitle => match ev {
                Event::Text(e) => {
                    let track = self
                        .tracks
                        .entry(self.current_track_id)
                        .or_insert(Track::new(self.current_id));
                    track.title = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::TrackTitle
                }

                Event::End(e) if e.local_name() == b"title" => ParserReadState::Track,

                _ => ParserReadState::TrackTitle,
            },

            ParserReadState::TrackPosition => match ev {
                Event::Text(e) => {
                    let track = self
                        .tracks
                        .entry(self.current_track_id)
                        .or_insert(Track::new(self.current_id));
                    track.position = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::TrackPosition
                }

                Event::End(e) if e.local_name() == b"position" => ParserReadState::Track,

                _ => ParserReadState::TrackPosition,
            },

            ParserReadState::TrackDuration => match ev {
                Event::Text(e) => {
                    let track = self
                        .tracks
                        .entry(self.current_track_id)
                        .or_insert(Track::new(self.current_id));
                    track.duration = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::TrackDuration
                }

                Event::End(e) if e.local_name() == b"duration" => ParserReadState::Track,

                _ => ParserReadState::TrackDuration,
            },

            ParserReadState::Companies => match ev {
                Event::End(e) if e.local_name() == b"companies" => ParserReadState::Release,

                _ => ParserReadState::Companies,
            },

            ParserReadState::Identifiers => match ev {
                Event::End(e) if e.local_name() == b"identifiers" => ParserReadState::Release,

                _ => ParserReadState::Identifiers,
            },

            ParserReadState::Artists => match ev {
                Event::End(e) if e.local_name() == b"artists" => ParserReadState::Release,

                _ => ParserReadState::Artists,
            },

            ParserReadState::ExtraArtists => match ev {
                Event::End(e) if e.local_name() == b"extraartists" => ParserReadState::Release,

                _ => ParserReadState::ExtraArtists,
            },

            // Just eat this
            ParserReadState::Images => match ev {
                Event::End(e) if e.local_name() == b"images" => ParserReadState::Release,

                _ => ParserReadState::Images,
            },

            ParserReadState::Formats => match ev {
                Event::Start(e) if e.local_name() == b"format" => {
                    let name: String = match e.attributes().find(|a| a.as_ref().unwrap().key == b"name") {
                        Some(Ok(a)) => str::parse(str::from_utf8(&a.value)?)?,
                        _ => "".to_string()
                    };
                    let qty: String = match e.attributes().find(|a| a.as_ref().unwrap().key == b"qty") {
                        Some(Ok(a)) => str::parse(str::from_utf8(&a.value)?)?,
                        _ => "".to_string()
                    };
                    let text: String = match e.attributes().find(|a| a.as_ref().unwrap().key == b"text") {
                        Some(Ok(a)) => str::parse(str::from_utf8(&a.value)?)?,
                        _ => "".to_string()
                    };

                    self.formats.insert(self.current_format_id, Format::new(self.current_id, name, qty, text));
                    ParserReadState::Format
                },

                Event::End(e) if e.local_name() == b"formats" => ParserReadState::Release,


                _ => ParserReadState::Formats,
            },

            ParserReadState::Format => match ev {
                Event::End(e) if e.local_name() == b"format" => {
                    self.current_format_id += 1;
                    ParserReadState::Formats
                },

                _ => ParserReadState::Format,
            },

            ParserReadState::Title => match ev {
                Event::Text(e) => {
                    self.current_release.title = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Title
                }

                Event::End(e) if e.local_name() == b"title" => ParserReadState::Release,

                _ => ParserReadState::Title,
            },

            ParserReadState::Country => match ev {
                Event::Text(e) => {
                    self.current_release.country = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Country
                }

                Event::End(e) if e.local_name() == b"country" => ParserReadState::Release,

                _ => ParserReadState::Country,
            },

            ParserReadState::Released => match ev {
                Event::Text(e) => {
                    self.current_release.released = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Released
                }

                Event::End(e) if e.local_name() == b"released" => ParserReadState::Release,

                _ => ParserReadState::Released,
            },

            ParserReadState::Notes => match ev {
                Event::Text(e) => {
                    self.current_release.notes = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Notes
                }

                Event::End(e) if e.local_name() == b"notes" => ParserReadState::Release,

                _ => ParserReadState::Notes,
            },

            ParserReadState::Genres => match ev {
                Event::Start(e) if e.local_name() == b"genre" => ParserReadState::Genre,

                Event::End(e) if e.local_name() == b"genres" => ParserReadState::Release,

                _ => ParserReadState::Genres,
            },

            ParserReadState::Genre => match ev {
                Event::Text(e) => {
                    self.current_release
                        .genres
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserReadState::Genre
                }
                Event::End(e) if e.local_name() == b"genre" => ParserReadState::Genres,

                _ => ParserReadState::Genre,
            },

            ParserReadState::Styles => match ev {
                Event::Start(e) if e.local_name() == b"style" => ParserReadState::Style,

                Event::End(e) if e.local_name() == b"styles" => ParserReadState::Release,

                _ => ParserReadState::Styles,
            },

            ParserReadState::Style => match ev {
                Event::Text(e) => {
                    self.current_release
                        .styles
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserReadState::Style
                }

                Event::End(e) if e.local_name() == b"style" => ParserReadState::Styles,

                _ => ParserReadState::Style,
            },

            ParserReadState::MasterId => match ev {
                Event::Text(e) => {
                    self.current_release.master_id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::MasterId
                }

                Event::End(e) if e.local_name() == b"master_id" => ParserReadState::Release,

                _ => ParserReadState::MasterId,
            },

            ParserReadState::DataQuality => match ev {
                Event::Text(e) => {
                    self.current_release.data_quality =
                        str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::DataQuality
                }

                Event::End(e) if e.local_name() == b"data_quality" => ParserReadState::Release,

                _ => ParserReadState::DataQuality,
            },

            // TODO Verify this is sufficient
            ParserReadState::Labels => match ev {
                Event::Empty(e) => {
                    let label_id = str::parse(str::from_utf8(
                        &e.attributes().nth(2).unwrap()?.unescaped_value()?,
                    )?)?;
                    self.release_labels.entry(label_id).or_insert(ReleaseLabel {
                        release_id: self.current_release.id,
                        label: str::parse(str::from_utf8(
                            &e.attributes().next().unwrap()?.unescaped_value()?,
                        )?)?,
                        catno: str::parse(str::from_utf8(
                            &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                        )?)?,
                        label_id: str::parse(str::from_utf8(
                            &e.attributes().nth(2).unwrap()?.unescaped_value()?,
                        )?)?,
                    });
                    ParserReadState::Labels
                }

                Event::End(e) if e.local_name() == b"labels" => ParserReadState::Release,

                _ => ParserReadState::Labels,
            },

            // TODO Fix this
            ParserReadState::Videos => match ev {
                Event::Start(e) if e.local_name() == b"video" => {
                    self.release_videos
                        .entry(self.current_video_id)
                        .or_insert(ReleaseVideo {
                            release_id: self.current_release.id,
                            duration: str::parse(str::from_utf8(
                                &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                            )?)?,
                            src: str::parse(str::from_utf8(
                                &e.attributes().next().unwrap()?.unescaped_value()?,
                            )?)?,
                            title: String::new(),
                        });
                    self.current_video_id += 1;
                    ParserReadState::Videos
                }

                Event::End(e) if e.local_name() == b"videos" => ParserReadState::Release,

                _ => ParserReadState::Videos,
            },
        };

        Ok(())
    }
}
