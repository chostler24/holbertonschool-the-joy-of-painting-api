DROP DATABASE IF EXISTS bobross;
CREATE DATABASE bobross;
USE bobross;

CREATE TABLE Colors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    hex VARCHAR(7) NOT NULL
);

CREATE TABLE Episodes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    season INT,
    episode INT,
    youtube_src VARCHAR(255),
    img_src VARCHAR(255),
);

CREATE TABLE Subjects (
    episode_id INT,
    subject_name VARCHAR(255),
    PRIMARY KEY (episode_id, subject_name),
    FOREIGN KEY (episode_id) REFERENCES Episodes(id)
);

CREATE TABLE PaintingColors (
    color_id INT,
    episode_id INT,
    PRIMARY KEY (color_id, episode_id),
    FOREIGN KEY (color_id) REFERENCES Colors(id),
    FOREIGN KEY (episode_id) REFERENCES Episodes(id)
);
