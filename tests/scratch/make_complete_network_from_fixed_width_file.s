RUN PGM = NETWORK MSG = "Read in network from fixed width file" 
FILEI LINKI[1] = %LINK_DATA_PATH%, VAR = model_link_id,1-7, shstGeometryId,9-40, A,42-47, B,49-54, distance,56-67, roadway,69-95, name,97-159, transit_access,161-161, drive_access,163-163, walk_access,165-165, bike_access,167-167, truck_access,169-169, area_type,171-171, county,173-178, centroidconnect,180-180, mpo,182-182, assign_group,184-185, roadway_class,187-188, AADT,190-195, trn_priority_AM,197-199, trn_priority_MD,201-203, trn_priority_PM,205-207, trn_priority_NT,209-211, ttime_assert_AM,213-215, ttime_assert_MD,217-219, ttime_assert_PM,221-223, ttime_assert_NT,225-227, lanes_AM,229-229, lanes_MD,231-231, lanes_PM,233-233, lanes_NT,235-235, access_AM,237-247, access_MD,249-259, access_PM,261-271, access_NT,273-283
FILEI NODEI[1] = %NODE_DATA_PATH%, VAR = model_node_id,1-6, osm_node_id,8-17, transit_node,19-19, drive_node,21-21, walk_node,23-23, bike_node,25-25, X,27-44, Y,46-63
FILEO NETO = "%SCENARIO_DIR%/complete_network.net" 
    ZONES = %zones% 
 
ENDRUN