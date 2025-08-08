CREATE TABLE public.channels (
    channel_id bigint NOT NULL,
    channel_name text NOT NULL
);

CREATE TABLE public.games (
    game_id integer NOT NULL,
    date timestamp with time zone,
    url text
);

CREATE SEQUENCE public.games_game_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.games_game_id_seq OWNED BY public.games.game_id;

CREATE TABLE public.ledgers (
    game_id integer NOT NULL,
    user_id text NOT NULL,
    net bigint,
    alias text
);

CREATE TABLE public.misc (
    misc_id bigint NOT NULL,
    misc_name text
);

CREATE TABLE public.players (
    player_id integer NOT NULL,
    name text,
    email text,
    discord_id bigint
);

CREATE SEQUENCE public.players_player_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.players_player_id_seq OWNED BY public.players.player_id;

CREATE TABLE public.roles (
    role_id bigint NOT NULL,
    role_name text
);

CREATE TABLE public.users (
    user_id text NOT NULL,
    player_id integer
);

ALTER TABLE ONLY public.games ALTER COLUMN game_id SET DEFAULT nextval('public.games_game_id_seq'::regclass);

ALTER TABLE ONLY public.players ALTER COLUMN player_id SET DEFAULT nextval('public.players_player_id_seq'::regclass);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_channel_name_key UNIQUE (channel_name);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (channel_id);

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_date_key UNIQUE (date);

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_pkey PRIMARY KEY (game_id);

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_url_key UNIQUE (url);

ALTER TABLE ONLY public.ledgers
    ADD CONSTRAINT ledgers_pkey PRIMARY KEY (game_id, user_id);

ALTER TABLE ONLY public.misc
    ADD CONSTRAINT misc_misc_name_key UNIQUE (misc_name);

ALTER TABLE ONLY public.misc
    ADD CONSTRAINT misc_pkey PRIMARY KEY (misc_id);

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_discord_id_key UNIQUE (discord_id);

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_pkey PRIMARY KEY (player_id);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT pokernow_ids_pkey PRIMARY KEY (user_id);

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (role_id);

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_role_name_key UNIQUE (role_name);

ALTER TABLE ONLY public.ledgers
    ADD CONSTRAINT fk_ledgers_games FOREIGN KEY (game_id) REFERENCES public.games(game_id) ON UPDATE CASCADE;

ALTER TABLE ONLY public.ledgers
    ADD CONSTRAINT fk_ledgers_users FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON UPDATE CASCADE;

ALTER TABLE ONLY public.users
    ADD CONSTRAINT fk_users_players FOREIGN KEY (player_id) REFERENCES public.players(player_id) ON UPDATE CASCADE;
