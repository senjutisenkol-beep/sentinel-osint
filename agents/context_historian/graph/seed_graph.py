import networkx as nx # type: ignore
import pickle
import os

def build_seed_graph():
    #G is the graph object holding all edges and nodes
    G = nx.DiGraph() #directed graph where edges have a direction.for  eg, operates_in goes from actor to region, not both ways. Direction matters for traversal
    # ── REGIONS ──────────────────────────────────────────────
    regions = [
        # Core Sahel states
        ('Mali',                {'type': 'Region', 'continent': 'Africa', 'capital': 'Bamako',          'note': 'Epicentre of Sahel insurgency'}),
        ('Niger',               {'type': 'Region', 'continent': 'Africa', 'capital': 'Niamey',          'note': 'Coup 2023, ECOWAS standoff'}),
        ('Burkina_Faso',        {'type': 'Region', 'continent': 'Africa', 'capital': 'Ouagadougou',     'note': 'Coup 2022, fastest deteriorating security'}),
        ('Chad',                {'type': 'Region', 'continent': 'Africa', 'capital': 'NDjamena',        'note': 'G5 Sahel member, relatively stable anchor'}),
        ('Mauritania',          {'type': 'Region', 'continent': 'Africa', 'capital': 'Nouakchott',      'note': 'G5 Sahel member, improved security since 2011'}),

        # Northern Africa
        ('Algeria',             {'type': 'Region', 'continent': 'Africa', 'capital': 'Algiers',         'note': 'AQIM origin state, closed border with Mali'}),
        ('Libya',               {'type': 'Region', 'continent': 'Africa', 'capital': 'Tripoli',         'note': 'State collapse 2011, weapons source for Sahel'}),
        ('Morocco',             {'type': 'Region', 'continent': 'Africa', 'capital': 'Rabat',           'note': 'Northern anchor, recruitement concern'}),

        # West Africa
        ('Nigeria',             {'type': 'Region', 'continent': 'Africa', 'capital': 'Abuja',           'note': 'Boko Haram origin, ISGS spillover risk'}),
        ('Senegal',             {'type': 'Region', 'continent': 'Africa', 'capital': 'Dakar',           'note': 'Relatively stable, hosts displaced populations'}),
        ('Guinea',              {'type': 'Region', 'continent': 'Africa', 'capital': 'Conakry',         'note': 'Coup 2021, instability spreading westward'}),
        ('Ivory_Coast',         {'type': 'Region', 'continent': 'Africa', 'capital': 'Abidjan',         'note': 'Southern spillover risk from Burkina Faso'}),
        ('Togo',                {'type': 'Region', 'continent': 'Africa', 'capital': 'Lome',            'note': 'Jihadist attacks in north since 2021'}),
        ('Benin',               {'type': 'Region', 'continent': 'Africa', 'capital': 'Porto-Novo',      'note': 'Jihadist incursions from Burkina Faso since 2021'}),

        # Sub-regions
        ('Northern_Mali',       {'type': 'Region', 'parent': 'Mali',          'note': 'Azawad — historic Tuareg homeland, JNIM stronghold'}),
        ('Central_Mali',        {'type': 'Region', 'parent': 'Mali',          'note': 'Mopti region, violence escalating since 2018'}),
        ('Kidal',               {'type': 'Region', 'parent': 'Northern_Mali', 'note': 'JNIM and MNLA stronghold, remote, hard to govern'}),
        ('Menaka',              {'type': 'Region', 'parent': 'Northern_Mali', 'note': 'ISGS stronghold, border with Niger'}),
        ('Gao',                 {'type': 'Region', 'parent': 'Northern_Mali', 'note': 'Strategic town, gateway to Niger'}),
        ('Liptako_Gourma',      {'type': 'Region', 'parent': 'Sahel',         'note': 'Tri-border zone Mali-Niger-Burkina, highest attack density globally'}),
        ('Sahel',               {'type': 'Region', 'note': 'Broader geographic and conflict zone across West Africa'}),
        ('Lake_Chad_Basin',     {'type': 'Region', 'note': 'Boko Haram and ISWAP theatre, Chad-Nigeria-Niger-Cameroon'}),
    ]
    G.add_nodes_from(regions)

    # ── ACTORS ───────────────────────────────────────────────
    actors = [
        ('GSPC',            {'type': 'Actor', 'full_name': 'Salafist Group for Preaching and Combat',          'founded': '1998', 'status': 'dissolved',  'dissolved': '2007', 'note': 'Became AQIM in 2007'}),
        ('AQIM',            {'type': 'Actor', 'full_name': 'Al-Qaeda in the Islamic Maghreb',                  'founded': '2007', 'status': 'active',     'ideology': 'jihadist', 'affiliate': 'Al-Qaeda'}),
        ('MNLA',            {'type': 'Actor', 'full_name': 'National Movement for the Liberation of Azawad',  'founded': '2011', 'status': 'active',     'ideology': 'separatist', 'ethnicity': 'Tuareg'}),
        ('Ansar_Dine',      {'type': 'Actor', 'full_name': 'Ansar Dine',                                       'founded': '2011', 'status': 'merged',     'note': 'Merged into JNIM 2017', 'ideology': 'jihadist'}),
        ('MUJAO',           {'type': 'Actor', 'full_name': 'Movement for Oneness and Jihad in West Africa',   'founded': '2011', 'status': 'merged',     'note': 'Merged into JNIM 2017', 'ideology': 'jihadist'}),
        ('ISGS',            {'type': 'Actor', 'full_name': 'Islamic State in the Greater Sahara',              'founded': '2015', 'status': 'active',     'ideology': 'jihadist', 'affiliate': 'ISIS'}),
        ('JNIM',            {'type': 'Actor', 'full_name': 'Jama\'at Nusrat al-Islam wal-Muslimin',           'founded': '2017', 'status': 'active',     'ideology': 'jihadist', 'affiliate': 'Al-Qaeda'}),
        ('GSIM',            {'type': 'Actor', 'full_name': 'Group to Support Islam and Muslims',               'founded': '2017', 'status': 'active',     'ideology': 'jihadist', 'note': 'Alternate name for JNIM'}),
        ('Wagner',          {'type': 'Actor', 'full_name': 'Wagner Group',                                     'founded': '2014', 'status': 'active',     'ideology': 'mercenary', 'note': 'Russian private military company'}),
        ('Mali_Junta',      {'type': 'Actor', 'full_name': 'National Committee for the Salvation of the People', 'founded': '2020', 'status': 'active',  'ideology': 'military', 'note': 'Ruling junta since 2020 coup'}),
        ('Niger_Junta',     {'type': 'Actor', 'full_name': 'National Council for the Safeguard of the Homeland', 'founded': '2023', 'status': 'active',  'ideology': 'military', 'note': 'Ruling junta since 2023 coup'}),
        ('Burkina_Junta',   {'type': 'Actor', 'full_name': 'Patriotic Movement for Safeguard and Restoration', 'founded': '2022', 'status': 'active',    'ideology': 'military', 'note': 'Ruling junta since 2022 coup'}),
        ('FAMA',            {'type': 'Actor', 'full_name': 'Forces Armees du Mali',                            'founded': '1960', 'status': 'active',     'ideology': 'state_military', 'note': 'Mali national armed forces'}),
        ('G5_Sahel_Force',  {'type': 'Actor', 'full_name': 'G5 Sahel Joint Force',                            'founded': '2017', 'status': 'degraded',   'note': 'Mali and Burkina withdrew after coups'}),
        ('Barkhane_Force',  {'type': 'Actor', 'full_name': 'Operation Barkhane',                               'founded': '2015', 'status': 'dissolved',  'dissolved': '2022', 'note': 'French regional counterterrorism force'}),
    ]
    G.add_nodes_from(actors)

    # ── EXTERNAL ACTORS ──────────────────────────────────────
    external_actors = [
        # Western states
        ('France',          {'type': 'ExternalActor', 'category': 'state',       'note': 'Primary Western intervener 2013-2022, expelled by Mali junta'}),
        ('USA',             {'type': 'ExternalActor', 'category': 'state',       'note': 'Special forces, drone base in Niger, AFRICOM operations'}),
        ('UK',              {'type': 'ExternalActor', 'category': 'state',       'note': 'Training missions, intelligence sharing with France'}),
        ('Germany',         {'type': 'ExternalActor', 'category': 'state',       'note': 'MINUSMA contributor, training mission in Mali'}),

        # Non-Western states
        ('Russia',          {'type': 'ExternalActor', 'category': 'state',       'note': 'Wagner deployment, filling vacuum left by France'}),
        ('China',           {'type': 'ExternalActor', 'category': 'state',       'note': 'Economic investment, infrastructure, MINUSMA contributor'}),
        ('Turkey',          {'type': 'ExternalActor', 'category': 'state',       'note': 'Arms sales to Sahel states, growing influence'}),
        ('UAE',             {'type': 'ExternalActor', 'category': 'state',       'note': 'Funding channels, political mediation in Sahel'}),
        ('Qatar',           {'type': 'ExternalActor', 'category': 'state',       'note': 'Hostage negotiations, alleged funding of armed groups'}),
        ('Algeria',         {'type': 'ExternalActor', 'category': 'state',       'note': 'Border security, mediation in Mali peace process — also a Region'}),

        # Multilateral institutions
        ('UN',              {'type': 'ExternalActor', 'category': 'institution', 'note': 'MINUSMA peacekeeping mission 2013-2023, withdrawn after Mali request'}),
        ('ECOWAS',          {'type': 'ExternalActor', 'category': 'institution', 'note': 'Regional body, sanctions and intervention threats post-coups'}),
        ('African_Union',   {'type': 'ExternalActor', 'category': 'institution', 'note': 'Conflict mediation, suspended Mali Burkina Niger after coups'}),
        ('EU',              {'type': 'ExternalActor', 'category': 'institution', 'note': 'EUTM Mali training mission, suspended 2022'}),
        ('IMF',             {'type': 'ExternalActor', 'category': 'institution', 'note': 'Economic conditionality, aid suspension post-coups'}),
        ('World_Bank',      {'type': 'ExternalActor', 'category': 'institution', 'note': 'Development funding, suspended disbursements after coups'}),
    ]
    G.add_nodes_from(external_actors)

    # ── EVENTS ───────────────────────────────────────────────
    events = [
        ('2003_GSPC_Kidnappings',       {'type': 'Event', 'date': '2003-02-01', 'location': 'Algeria',       'description': 'GSPC kidnaps 32 European tourists, first major Sahel hostage crisis'}),
        ('2007_AQIM_Founding',          {'type': 'Event', 'date': '2007-01-25', 'location': 'Algeria',       'description': 'GSPC rebrands as AQIM, al-Qaeda formally enters the Sahel'}),
        ('2009_Operation_Flintlock',    {'type': 'Event', 'date': '2009-02-01', 'location': 'Sahel',         'description': 'US-led annual military exercise signalling Western interest in Sahel security'}),
        ('2011_Libya_Collapse',         {'type': 'Event', 'date': '2011-10-20', 'location': 'Libya',         'description': 'Gaddafi killed, state collapses, weapons and fighters flood into Sahel'}),
        ('2011_MNLA_Founded',           {'type': 'Event', 'date': '2011-10-16', 'location': 'Northern_Mali', 'description': 'Tuareg separatists form MNLA, armed by returning Libya fighters'}),
        ('2012_Tuareg_Rebellion',       {'type': 'Event', 'date': '2012-01-17', 'location': 'Northern_Mali', 'description': 'MNLA-led rebellion seizing Northern Mali'}),
        ('2012_Mali_Coup',              {'type': 'Event', 'date': '2012-03-22', 'location': 'Mali',          'description': 'Military coup ousting President Toure amid rebellion crisis'}),
        ('2013_Operation_Serval',       {'type': 'Event', 'date': '2013-01-11', 'location': 'Mali',          'description': 'French military intervention pushing back jihadist advance on Bamako'}),
        ('2015_ISGS_Founded',           {'type': 'Event', 'date': '2015-05-01', 'location': 'Sahel',         'description': 'ISIS splinter breaks from AQIM, second major jihadist faction in Sahel'}),
        ('2015_Operation_Barkhane',     {'type': 'Event', 'date': '2015-08-01', 'location': 'Sahel',         'description': 'France expands Serval into regional Barkhane operation across G5 Sahel'}),
        ('2017_G5_Sahel_Force',         {'type': 'Event', 'date': '2017-07-02', 'location': 'Sahel',         'description': 'G5 Sahel Joint Force created — Mali, Niger, Burkina, Chad, Mauritania'}),
        ('2017_JNIM_Formation',         {'type': 'Event', 'date': '2017-03-02', 'location': 'Mali',          'description': 'Merger of four jihadist groups under AQIM umbrella'}),
        ('2020_Mali_Coup_1',            {'type': 'Event', 'date': '2020-08-18', 'location': 'Mali',          'description': 'First coup — President IBK ousted by military junta'}),
        ('2021_Mali_Coup_2',            {'type': 'Event', 'date': '2021-05-24', 'location': 'Mali',          'description': 'Second coup — transitional president removed, junta consolidates power'}),
        ('2022_France_Withdrawal',      {'type': 'Event', 'date': '2022-08-15', 'location': 'Mali',          'description': 'France expelled by Mali junta, Operation Barkhane ends'}),
        ('2023_Wagner_Deployment',      {'type': 'Event', 'date': '2023-01-01', 'location': 'Mali',          'description': 'Wagner Group deployed to Mali replacing French forces'}),
    ]
    G.add_nodes_from(events)

    # ── CONFLICTS ─────────────────────────────────────────────
    conflicts = [
        ('Algeria_Insurgency',          {'type': 'Conflict', 'since': '1991', 'status': 'low_level',  'description': 'Post-civil war jihadist activity in Algeria, direct precursor to AQIM'}),
        ('Tuareg_Separatism',           {'type': 'Conflict', 'since': '1960', 'status': 'recurring',  'description': 'Recurring Tuareg separatist conflict in Northern Mali and Niger'}),
        ('Northern_Mali_Conflict',      {'type': 'Conflict', 'since': '2012', 'status': 'active',     'description': 'Armed conflict in Northern Mali between state, separatists, and jihadists'}),
        ('Sahel_Insurgency',            {'type': 'Conflict', 'since': '2012', 'status': 'active',     'description': 'Broader jihadist insurgency spreading across Mali, Burkina Faso, Niger'}),
        ('Liptako_Gourma_Crisis',       {'type': 'Conflict', 'since': '2015', 'status': 'active',     'description': 'Tri-border insurgency at Mali, Niger, Burkina Faso junction — highest violence density'}),
        ('Burkina_Insurgency',          {'type': 'Conflict', 'since': '2015', 'status': 'active',     'description': 'Jihadist expansion from Mali into Burkina Faso, accelerating after 2019'}),
        ('Niger_Spillover',             {'type': 'Conflict', 'since': '2017', 'status': 'active',     'description': 'ISGS and JNIM expansion into Niger, pushing toward Nigerian border'}),
        ('Mali_Political_Crisis',       {'type': 'Conflict', 'since': '2020', 'status': 'active',     'description': 'Political instability following two coups, junta consolidating power'}),
        ('Wagner_Proxy_Conflict',       {'type': 'Conflict', 'since': '2023', 'status': 'active',     'description': 'Russian influence operation via Wagner replacing Western security presence'}),
        ('ECOWAS_Standoff',             {'type': 'Conflict', 'since': '2023', 'status': 'resolved',   'description': 'ECOWAS threatened military intervention after Niger coup, standoff de-escalated'}),
    ]
    G.add_nodes_from(conflicts)

    # ── EDGES: BORDERS ───────────────────────────────────────
    borders = [
        ('Mali',        'Niger',        {'relationship': 'borders', 'border_type': 'land', 'note': 'Liptako-Gourma zone, porous'}),
        ('Mali',        'Burkina_Faso', {'relationship': 'borders', 'border_type': 'land', 'note': 'High insurgency spillover'}),
        ('Mali',        'Algeria',      {'relationship': 'borders', 'border_type': 'land', 'note': 'Algeria closed border 2020'}),
        ('Mali',        'Mauritania',   {'relationship': 'borders', 'border_type': 'land', 'note': 'Relatively stable crossing'}),
        ('Mali',        'Senegal',      {'relationship': 'borders', 'border_type': 'land', 'note': 'Western border, stable'}),
        ('Mali',        'Guinea',       {'relationship': 'borders', 'border_type': 'land', 'note': 'Southern border'}),
        ('Mali',        'Ivory_Coast',  {'relationship': 'borders', 'border_type': 'land', 'note': 'Spillover risk increasing'}),
        ('Niger',       'Burkina_Faso', {'relationship': 'borders', 'border_type': 'land', 'note': 'Liptako-Gourma tri-border'}),
        ('Niger',       'Nigeria',      {'relationship': 'borders', 'border_type': 'land', 'note': 'ISGS expansion route'}),
        ('Niger',       'Algeria',      {'relationship': 'borders', 'border_type': 'land', 'note': 'Northern desert border'}),
        ('Niger',       'Chad',         {'relationship': 'borders', 'border_type': 'land', 'note': 'Lake Chad Basin proximity'}),
        ('Burkina_Faso','Ivory_Coast',  {'relationship': 'borders', 'border_type': 'land', 'note': 'Spillover risk since 2021'}),
        ('Burkina_Faso','Togo',         {'relationship': 'borders', 'border_type': 'land', 'note': 'Jihadist incursions 2021+'}),
        ('Burkina_Faso','Benin',        {'relationship': 'borders', 'border_type': 'land', 'note': 'Jihadist incursions 2021+'}),
        ('Libya',       'Algeria',      {'relationship': 'borders', 'border_type': 'land', 'note': 'Weapons smuggling route post-2011'}),
        ('Libya',       'Niger',        {'relationship': 'borders', 'border_type': 'land', 'note': 'Migration and arms route'}),
        ('Northern_Mali','Kidal',       {'relationship': 'borders', 'border_type': 'subregion', 'note': 'Kidal is within Northern Mali'}),
        ('Northern_Mali','Menaka',      {'relationship': 'borders', 'border_type': 'subregion', 'note': 'Menaka is within Northern Mali'}),
        ('Northern_Mali','Gao',         {'relationship': 'borders', 'border_type': 'subregion', 'note': 'Gao is within Northern Mali'}),
    ]
    G.add_edges_from(borders)

    # ── EDGES: OPERATES_IN ───────────────────────────────────
    operates_in = [
        ('JNIM',            'Northern_Mali',    {'relationship': 'operates_in', 'since': '2017', 'presence_type': 'active',    'note': 'Primary stronghold'}),
        ('JNIM',            'Kidal',            {'relationship': 'operates_in', 'since': '2017', 'presence_type': 'active',    'note': 'Deep entrenchment'}),
        ('JNIM',            'Central_Mali',     {'relationship': 'operates_in', 'since': '2018', 'presence_type': 'active',    'note': 'Expanding into Mopti region'}),
        ('JNIM',            'Liptako_Gourma',   {'relationship': 'operates_in', 'since': '2017', 'presence_type': 'active',    'note': 'Tri-border operations'}),
        ('JNIM',            'Burkina_Faso',     {'relationship': 'operates_in', 'since': '2019', 'presence_type': 'active',    'note': 'Expanding westward'}),
        ('ISGS',            'Menaka',           {'relationship': 'operates_in', 'since': '2015', 'presence_type': 'active',    'note': 'Primary stronghold'}),
        ('ISGS',            'Liptako_Gourma',   {'relationship': 'operates_in', 'since': '2015', 'presence_type': 'active',    'note': 'Competing with JNIM'}),
        ('ISGS',            'Niger',            {'relationship': 'operates_in', 'since': '2017', 'presence_type': 'active',    'note': 'Expanding toward Nigerian border'}),
        ('AQIM',            'Algeria',          {'relationship': 'operates_in', 'since': '2007', 'presence_type': 'reduced',   'note': 'Origin base, reduced after Algerian pressure'}),
        ('AQIM',            'Northern_Mali',    {'relationship': 'operates_in', 'since': '2007', 'presence_type': 'active',    'note': 'Sahel expansion base'}),
        ('MNLA',            'Northern_Mali',    {'relationship': 'operates_in', 'since': '2011', 'presence_type': 'active',    'note': 'Tuareg separatist homeland'}),
        ('MNLA',            'Kidal',            {'relationship': 'operates_in', 'since': '2012', 'presence_type': 'active',    'note': 'Administrative control post-rebellion'}),
        ('Wagner',          'Mali',             {'relationship': 'operates_in', 'since': '2023', 'presence_type': 'active',    'note': 'Deployed replacing French forces'}),
        ('Wagner',          'Northern_Mali',    {'relationship': 'operates_in', 'since': '2023', 'presence_type': 'active',    'note': 'Joint operations with FAMA'}),
        ('FAMA',            'Mali',             {'relationship': 'operates_in', 'since': '1960', 'presence_type': 'active',    'note': 'National armed forces'}),
        ('FAMA',            'Northern_Mali',    {'relationship': 'operates_in', 'since': '2012', 'presence_type': 'contested', 'note': 'Contested control with jihadists'}),
        ('Mali_Junta',      'Mali',             {'relationship': 'operates_in', 'since': '2020', 'presence_type': 'active',    'note': 'Political control since coup'}),
        ('Niger_Junta',     'Niger',            {'relationship': 'operates_in', 'since': '2023', 'presence_type': 'active',    'note': 'Political control since coup'}),
        ('Burkina_Junta',   'Burkina_Faso',     {'relationship': 'operates_in', 'since': '2022', 'presence_type': 'active',    'note': 'Political control since coup'}),
        ('G5_Sahel_Force',  'Liptako_Gourma',  {'relationship': 'operates_in', 'since': '2017', 'presence_type': 'degraded',  'note': 'Mali and Burkina withdrew after coups'}),
        ('Barkhane_Force',  'Sahel',            {'relationship': 'operates_in', 'since': '2015', 'presence_type': 'dissolved', 'end': '2022', 'note': 'Expelled by Mali junta'}),
        ('GSPC',            'Algeria',          {'relationship': 'operates_in', 'since': '1998', 'presence_type': 'dissolved', 'end': '2007', 'note': 'Became AQIM'}),
    ]
    G.add_edges_from(operates_in)

    # ── EDGES: AFFILIATED_WITH ───────────────────────────────
    affiliated_with = [
        ('JNIM',    'AQIM',         {'relationship': 'affiliated_with', 'type': 'parent',   'since': '2017', 'note': 'JNIM is AQIM affiliate in Sahel'}),
        ('GSIM',    'AQIM',         {'relationship': 'affiliated_with', 'type': 'parent',   'since': '2017', 'note': 'GSIM alternate name for JNIM'}),
        ('ISGS',    'AQIM',         {'relationship': 'affiliated_with', 'type': 'splinter',  'since': '2015', 'note': 'Split from AQIM to join ISIS'}),
        ('GSPC',    'AQIM',         {'relationship': 'affiliated_with', 'type': 'precursor', 'since': '2007', 'end': '2007', 'note': 'GSPC became AQIM'}),
        ('Ansar_Dine', 'AQIM',      {'relationship': 'affiliated_with', 'type': 'ally',     'since': '2012', 'end': '2017', 'note': 'Merged into JNIM 2017'}),
        ('MUJAO',   'AQIM',         {'relationship': 'affiliated_with', 'type': 'splinter',  'since': '2011', 'end': '2017', 'note': 'Merged into JNIM 2017'}),
        ('Wagner',  'Mali_Junta',   {'relationship': 'affiliated_with', 'type': 'contractor','since': '2023', 'note': 'Wagner contracted by Mali junta'}),
        ('Wagner',  'Niger_Junta',  {'relationship': 'affiliated_with', 'type': 'contractor','since': '2023', 'note': 'Wagner expanding to Niger'}),
        ('FAMA',    'Mali_Junta',   {'relationship': 'affiliated_with', 'type': 'state_military', 'since': '2020', 'note': 'Armed forces under junta command'}),
    ]
    G.add_edges_from(affiliated_with)

    # ── EDGES: INTERVENES_IN ─────────────────────────────────
    intervenes_in = [
        ('France',          'Mali',         {'relationship': 'intervenes_in', 'type': 'military',   'start': '2013-01', 'end': '2022-08', 'operation': 'Serval then Barkhane'}),
        ('France',          'Burkina_Faso', {'relationship': 'intervenes_in', 'type': 'military',   'start': '2015-01', 'end': '2022-12', 'operation': 'Barkhane'}),
        ('France',          'Niger',        {'relationship': 'intervenes_in', 'type': 'military',   'start': '2015-01', 'end': '2023-12', 'operation': 'Barkhane — expelled after coup'}),
        ('USA',             'Mali',         {'relationship': 'intervenes_in', 'type': 'military',   'start': '2013-01', 'end': '2023-01', 'operation': 'AFRICOM special forces support'}),
        ('USA',             'Niger',        {'relationship': 'intervenes_in', 'type': 'military',   'start': '2013-01', 'operation': 'Drone base Agadez — status uncertain post-coup'}),
        ('USA',             'Sahel',        {'relationship': 'intervenes_in', 'type': 'military',   'start': '2002-01', 'operation': 'Operation Flintlock annual exercises'}),
        ('UN',              'Mali',         {'relationship': 'intervenes_in', 'type': 'peacekeeping','start': '2013-07', 'end': '2023-12', 'operation': 'MINUSMA — withdrawn after Mali request'}),
        ('EU',              'Mali',         {'relationship': 'intervenes_in', 'type': 'training',   'start': '2013-01', 'end': '2022-12', 'operation': 'EUTM Mali — suspended after coup'}),
        ('Russia',          'Mali',         {'relationship': 'intervenes_in', 'type': 'military',   'start': '2023-01', 'operation': 'Wagner deployment replacing France'}),
        ('Russia',          'Burkina_Faso', {'relationship': 'intervenes_in', 'type': 'military',   'start': '2023-06', 'operation': 'Wagner deployment after junta request'}),
        ('Russia',          'Niger',        {'relationship': 'intervenes_in', 'type': 'military',   'start': '2024-01', 'operation': 'Wagner expanding post-coup'}),
        ('Algeria',         'Mali',         {'relationship': 'intervenes_in', 'type': 'diplomatic', 'start': '2014-01', 'operation': 'Algiers Peace Process mediator'}),
        ('ECOWAS',          'Mali',         {'relationship': 'intervenes_in', 'type': 'diplomatic', 'start': '2020-08', 'operation': 'Sanctions and mediation post-coup'}),
        ('ECOWAS',          'Niger',        {'relationship': 'intervenes_in', 'type': 'diplomatic', 'start': '2023-07', 'end': '2023-12', 'operation': 'Threatened military intervention — standoff resolved'}),
        ('African_Union',   'Mali',         {'relationship': 'intervenes_in', 'type': 'diplomatic', 'start': '2012-01', 'operation': 'Conflict mediation and suspension'}),
        ('China',           'Mali',         {'relationship': 'intervenes_in', 'type': 'economic',   'start': '2010-01', 'operation': 'Infrastructure investment and MINUSMA contribution'}),
        ('Turkey',          'Sahel',        {'relationship': 'intervenes_in', 'type': 'economic',   'start': '2018-01', 'operation': 'Arms sales and diplomatic outreach'}),
        ('Qatar',           'Mali',         {'relationship': 'intervenes_in', 'type': 'diplomatic', 'start': '2013-01', 'operation': 'Hostage negotiations and mediation'}),
        ('Germany',         'Mali',         {'relationship': 'intervenes_in', 'type': 'military',   'start': '2013-07', 'end': '2023-12', 'operation': 'MINUSMA and EUTM contributor'}),
    ]
    G.add_edges_from(intervenes_in)

    # ── EDGES: SUPPORTS ──────────────────────────────────────
    supports = [
        ('Russia',  'Wagner',       {'relationship': 'supports', 'type': 'covert',      'since': '2014', 'note': 'State backing of Wagner PMC'}),
        ('Russia',  'Mali_Junta',   {'relationship': 'supports', 'type': 'diplomatic',  'since': '2020', 'note': 'Political backing at UN Security Council'}),
        ('Russia',  'Niger_Junta',  {'relationship': 'supports', 'type': 'diplomatic',  'since': '2023', 'note': 'Blocking ECOWAS intervention at UN'}),
        ('Russia',  'Burkina_Junta',{'relationship': 'supports', 'type': 'diplomatic',  'since': '2022', 'note': 'Anti-Western alignment'}),
        ('Qatar',   'JNIM',         {'relationship': 'supports', 'type': 'alleged',     'since': '2012', 'note': 'Alleged funding — disputed, used in hostage negotiations'}),
        ('UAE',     'Mali_Junta',   {'relationship': 'supports', 'type': 'economic',    'since': '2021', 'note': 'Financial support to junta'}),
        ('Algeria', 'MNLA',         {'relationship': 'supports', 'type': 'diplomatic',  'since': '2014', 'note': 'Mediation support for Tuareg political process'}),
        ('France',  'G5_Sahel_Force',{'relationship': 'supports','type': 'military',    'since': '2017', 'end': '2022', 'note': 'Funding and training G5 force'}),
        ('USA',     'G5_Sahel_Force',{'relationship': 'supports','type': 'military',    'since': '2017', 'note': 'Funding and equipment'}),
        ('EU',      'G5_Sahel_Force',{'relationship': 'supports','type': 'economic',    'since': '2017', 'note': 'Development funding'}),
        ('Turkey',  'Niger_Junta',  {'relationship': 'supports', 'type': 'diplomatic',  'since': '2023', 'note': 'Arms sales and anti-ECOWAS stance'}),
    ]
    G.add_edges_from(supports)

    # ── EDGES: CAUSED_BY ─────────────────────────────────────
    caused_by = [
        ('2007_AQIM_Founding',      '2003_GSPC_Kidnappings',    {'relationship': 'caused_by', 'mechanism': 'organisational evolution', 'date': '2007-01-25'}),
        ('2011_MNLA_Founded',       '2011_Libya_Collapse',      {'relationship': 'caused_by', 'mechanism': 'weapons and fighters returned from Libya', 'date': '2011-10'}),
        ('2012_Tuareg_Rebellion',   '2011_MNLA_Founded',        {'relationship': 'caused_by', 'mechanism': 'MNLA launched rebellion after formation', 'date': '2012-01'}),
        ('2012_Tuareg_Rebellion',   '2011_Libya_Collapse',      {'relationship': 'caused_by', 'mechanism': 'weapons flooded into Mali from Libya', 'date': '2012-01'}),
        ('2012_Mali_Coup',          '2012_Tuareg_Rebellion',    {'relationship': 'caused_by', 'mechanism': 'military blamed government for battlefield losses', 'date': '2012-03'}),
        ('2013_Operation_Serval',   '2012_Tuareg_Rebellion',    {'relationship': 'caused_by', 'mechanism': 'jihadist advance on Bamako triggered French intervention', 'date': '2013-01'}),
        ('2015_ISGS_Founded',       '2007_AQIM_Founding',       {'relationship': 'caused_by', 'mechanism': 'ideological splinter pledging allegiance to ISIS', 'date': '2015-05'}),
        ('2015_Operation_Barkhane', '2013_Operation_Serval',    {'relationship': 'caused_by', 'mechanism': 'Serval expanded into permanent regional operation', 'date': '2015-08'}),
        ('2017_JNIM_Formation',     '2007_AQIM_Founding',       {'relationship': 'caused_by', 'mechanism': 'AQIM orchestrated merger of four Sahel groups', 'date': '2017-03'}),
        ('2017_JNIM_Formation',     '2012_Tuareg_Rebellion',    {'relationship': 'caused_by', 'mechanism': 'power vacuum in Northern Mali enabled jihadist consolidation', 'date': '2017-03'}),
        ('2017_G5_Sahel_Force',     '2015_Operation_Barkhane',  {'relationship': 'caused_by', 'mechanism': 'France pushed regional states to build joint force', 'date': '2017-07'}),
        ('2020_Mali_Coup_1',        '2012_Tuareg_Rebellion',    {'relationship': 'caused_by', 'mechanism': 'years of military failures eroded government legitimacy', 'date': '2020-08'}),
        ('2021_Mali_Coup_2',        '2020_Mali_Coup_1',         {'relationship': 'caused_by', 'mechanism': 'transitional government failed to satisfy junta demands', 'date': '2021-05'}),
        ('2022_France_Withdrawal',  '2021_Mali_Coup_2',         {'relationship': 'caused_by', 'mechanism': 'junta expelled French forces after diplomatic breakdown', 'date': '2022-08'}),
        ('2023_Wagner_Deployment',  '2022_France_Withdrawal',   {'relationship': 'caused_by', 'mechanism': 'security vacuum filled by Wagner after French exit', 'date': '2023-01'}),
    ]
    G.add_edges_from(caused_by)

    # ── EDGES: PART_OF ───────────────────────────────────────
    part_of = [
        ('Northern_Mali_Conflict',  'Sahel_Insurgency',     {'relationship': 'part_of', 'since': '2012', 'note': 'Northern Mali is the origin theatre'}),
        ('Liptako_Gourma_Crisis',   'Sahel_Insurgency',     {'relationship': 'part_of', 'since': '2015', 'note': 'Tri-border has become highest intensity zone'}),
        ('Burkina_Insurgency',      'Sahel_Insurgency',     {'relationship': 'part_of', 'since': '2015', 'note': 'Burkina became second major theatre'}),
        ('Niger_Spillover',         'Sahel_Insurgency',     {'relationship': 'part_of', 'since': '2017', 'note': 'Niger increasingly drawn into broader conflict'}),
        ('Mali_Political_Crisis',   'Sahel_Insurgency',     {'relationship': 'part_of', 'since': '2020', 'note': 'Political instability fuels insurgency'}),
        ('Wagner_Proxy_Conflict',   'Sahel_Insurgency',     {'relationship': 'part_of', 'since': '2023', 'note': 'Great power competition layered on insurgency'}),
        ('Algeria_Insurgency',      'Northern_Mali_Conflict',{'relationship': 'part_of', 'since': '2007', 'note': 'AQIM spillover from Algeria into Mali'}),
        ('Tuareg_Separatism',       'Northern_Mali_Conflict',{'relationship': 'part_of', 'since': '2012', 'note': 'Tuareg separatism is a parallel conflict strand'}),
        ('ECOWAS_Standoff',         'Mali_Political_Crisis', {'relationship': 'part_of', 'since': '2023', 'note': 'Regional response to coup wave'}),
        ('Lake_Chad_Basin',         'Sahel',                 {'relationship': 'part_of', 'since': '2009', 'note': 'Lake Chad Basin is eastern extension of Sahel crisis'}),
    ]
    G.add_edges_from(part_of)

    return G


def save_graph(G, path='agents/context_historian/graph/knowledge_graph.pkl'):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        pickle.dump(G, f)
    print(f'Graph saved: {len(G.nodes)} nodes, {len(G.edges)} edges')


def load_graph(path='agents/context_historian/graph/knowledge_graph.pkl'):
    with open(path, 'rb') as f:
        return pickle.load(f)


if __name__ == '__main__':
    G = build_seed_graph()
    save_graph(G)

