# Use case: A user has a file on one srm resource (e.g Nebraska and he
# wants that this file to be replicated on all other srm resources
# that are currently "up" and have enough space.

# we need an irods rule that could be ran by user and will do the
#  following:

# a. user provides the following input:
# user name (e.g yzheng)
# file name (e.g test_1)
# collection_name (/ogs/home/yzheng)
# resource group (osgSrmGroup)

# b. the rule checks that file exists on at least on srm resource. If
#not: exists with an error message

# c. if a file exists on more than one resource, verifies that file
# sizes are the same (We don't save checksum for now), if sizes are
# not the same rule exists with an error message

# d. for all resources that are "up" and have enough space (remaining
#  quota is > file size) and dont' have a copy of this file: copy the
#  file

# e. delete the file from disk cache

# Author: Yaling Zheng
# Date: May 8th, 2012


SafeReplicateRule{
	writeLine("stdout", "the parameters values are: UserName= *UserName, CollectionName= *CollectionName, ResourceGroup= *ResourceGroup, FileName= *FileName");
	# initialize *Path
	*Path = "*CollectionName"++"/"++"*FileName";
	# retrieve the user group name
	*condition_q0 = "USER_NAME = '*UserName' and USER_GROUP_NAME not like '*UserName' and COLL_NAME = '*CollectionName'";
	msiMakeQuery("USER_NAME, USER_GROUP_NAME", *condition_q0, *Query0);
	msiExecStrCondQuery(*Query0, *QueryOut0);
	msiGetContInxFromGenQueryOut(*QueryOut0, *ContInx_q0);
	# writeLine("stdout", "ContInx_q0 = *ContInx_q0");
	*usergroup = "";
	foreach (*QueryOut0){
		msiGetValByKey(*QueryOut0, "USER_GROUP_NAME", *usergroup);
		# writeLine("stdout", "usergroup = *usergroup");
	}
	# writeLine("stdout", "usergroup = *usergroup");
	# looking for resources that file exist in
	*condition_q1 = "USER_NAME = '*UserName' and DATA_NAME = '*FileName' and COLL_NAME = '*CollectionName' and RESC_GROUP_NAME = '*ResourceGroup'";
	msiMakeQuery("RESC_NAME, DATA_SIZE, DATA_REPL_NUM", *condition_q1, *Query1);
	msiExecStrCondQuery(*Query1, *QueryOut1);
	msiGetContInxFromGenQueryOut(*QueryOut1, *ContInx_q1);
	# writeLine("stdout", "ContInx_q1 = *ContInx_q1");
	*file_size = -1;
	*numberResourcesContainFile = 0;
	*fileConsistency = true;
	foreach (*QueryOut1){
	 	msiGetValByKey(*QueryOut1, "RESC_NAME", *currentresourcename);
	 	writeLine("stdout", "*currentresourcename contains *FileName");
	 	msiGetValByKey(*QueryOut1, "DATA_SIZE", *new_file_size);
	 	if (*file_size < 0){
	 	   *file_size = int(*new_file_size);
	 	}
	 	else{ # compare *file_size with *new_file_size
	 	      if (int(*new_file_size) != *file_size){
	 	      	 *fileConsistency = false;
	 	      		    }
	 	      }
	 	*numberResourcesContainFile = *numberResourcesContainFile + 1;
	}
	 *exit_flag = false;
	 # if no resource contain this file, we exit
	 if (*numberResourcesContainFile == 0){
	    writeLine("stdout", "No resource of the resource group *ResourceGroup contain this file *FileName");
	    *exit_flag = true;  
	 }
	 if (*fileConsistency == false){
	    writeLine("stdout", "File sizes on different resources are not consistent ... ");
	    *exit_flag = true;
	 }
	if (*exit_flag == false) {
	    *Qs = 0 - int(*file_size);
	    *condition_q2 = "RESC_STATUS = 'up' and RESC_TYPE_NAME = 'MSS universal driver' and RESC_GROUP_NAME = '*ResourceGroup'";
	    msiMakeQuery("RESC_NAME", *condition_q2, *Query2);
	    msiExecStrCondQuery(*Query2, *QueryOut2);
	    msiGetContInxFromGenQueryOut(*QueryOut2, *ContInx_q2);
	    # writeLine("stdout", "ContInx_q2 = *ContInx_q2");
	    foreach (*QueryOut2){
	 	msiGetValByKey(*QueryOut2, "RESC_NAME", *currentresourcename);
	 	writeLine("stdout", "*currentresourcename is up");
	 	# now, we want to check whether this resource is within quota and quota user name is the User Group
	 	*condition_q3 = "QUOTA_RESC_NAME not like 'UCSDT2' and QUOTA_RESC_NAME = '*currentresourcename' and QUOTA_OVER <= '*Qs' and QUOTA_USER_NAME = '*usergroup'";
	 	msiMakeQuery("QUOTA_RESC_NAME", *condition_q3, *Query3);
	 	msiExecStrCondQuery(*Query3, *QueryOut3);
	 	msiGetContInxFromGenQueryOut(*QueryOut3, *ContInx_q3);
	 	# writeLine("stdout", "ContInx_q3 = *ContInx_q3");
	 	foreach (*QueryOut3){
	 		msiGetValByKey(*QueryOut3, "QUOTA_RESC_NAME", *resourcename);
	 		writeLine("stdout", "*resourcename has enough quota ...");
	 		*condition_q4 = "DATA_NAME = '*FileName' and RESC_NAME = '*resourcename'";
	 		msiMakeQuery("RESC_NAME", *condition_q4, *Query4);
	 		msiExecStrCondQuery(*Query4, *QueryOut4);
	 		msiGetContInxFromGenQueryOut(*QueryOut4, *ContInx_q4);
	 		# writeLine("stdout", "ContInx_q4 = *ContInx_q4");
	 		*ResourceContainFileFlag = false;
	 		foreach (*QueryOut4){
	 			msiGetValByKey(*QueryOut4, "RESC_NAME", *finalresourcename);
	 			writeLine("stdout", "resource *finalresourcename contains *FileName");	
	 			*ResourceContainFileFlag = true;
	 		}
			# writeLine("stdout", "ResourceContainFileFlag = *ResourceContainFileFlag");
	 		if (*ResourceContainFileFlag == false){
	 		   writeLine("stdout", "resource *resourcename does not contain *FileName ... preparing to copy the file into this resource ...");
	 		   # Now, copy this resource
	 		   *returnresult = msiDataObjReplWithOptions(*Path, *resourcename, "irods", *Status);
	 		   # writeLine("stdout", "returnresult = *returnresult");
	 		   if (*returnresult >= 0){	
	 	   	      writeLine("stdout", "The file *FileName has been successfully replicated to resource *resourcename");
	 		      writeLine("stdout", "The file *FileName will be deleted from cache...");
	                      *condition_q5 = "USER_NAME = '*UserName' and DATA_NAME = '*FileName' and RESC_NAME = 'diskCache'";
	 		      msiMakeQuery("RESC_NAME, DATA_NAME, DATA_REPL_NUM", *condition_q5, *Query5);
	 		      msiExecStrCondQuery(*Query5, *QueryOut5);
	 		      msiGetContInxFromGenQueryOut(*QueryOut5, *ContInx_q5);
	 		      # writeLine("stdout", "ContInx_q5 = *ContInx_q5");
	 		      *replica = 0;
	 		      foreach (*QueryOut5){
	 		      		msiGetValByKey(*QueryOut5, "DATA_REPL_NUM", *replica);
	 				# writeLine("stdout", "replNum = *replica")				
	 				}
	 		      # writeLine("stdout", "final replNum = *replica")
	 		      *removeResult = msiDataObjUnlink("objPath=*Path++++replNum=*replica", *Status);
	 		      if (*removeResult==0){
	 		      	 writeLine("stdout", "Successfully remove the file *FileName from disk Cache ...");
	 			 }
	 		      else{
	 			 writeLine("stdout", "Failed to remove the file *FileName from disk Cache ");
	 			}
	 		      }
	 		 else {
	 		      writeLine("stdout", "Failed to replicate file *FileName to *resourcename");
	 		      }
	 		 }
	 		}
	 	}
	 }
}

input *UserName="yzheng", *FileName="hello3.txt", *CollectionName="/osg/home/yzheng", *ResourceGroup = "osgSrmGroup"
output ruleExecOut 

