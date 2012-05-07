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

SafeReplicateRule{
	# looking for resources that file exist in
	*condition_q = "USER_NAME = '*UserName' and DATA_NAME = '*FileName' and COLL_NAME = '*CollectionName' and RESC_GROUP_NAME = '*ResourceGroup'";
	msiMakeQuery("RESC_NAME, DATA_SIZE", *condition_q, *Query);
	msiExecStrCondQuery(*Query, *QueryOut);
	msiGetContInxFromGenQueryOut(*QueryOut, *ContInx_q);
	if (*ContInx_q < 0){
	   writeLine("serverLog", "The file *FileName does not exist in at least one resource in resource group *ResourceGroup");
	   # when file does not exist in at least in even one resource, we exist with writing an error message on serverLog
	}
	else {	
	   if (*ContInx_q > 1){
	      # we need to know the FILE_SIZE in each resource
	      *file_size = -1;
	      *forloop_flag = true;
	      foreach (*QueryOut){
	        if (*forloop_flag){
		   if (file_size >=0){
		        msiGetValByKey(*QueryOut, "DATA_SIZE", *new_file_size);
		     	msiGetValByKey(*QueryOut, "DATA_REPL_NUM", *Replica);
		     	if (*new_file_size!=*file_size){
		     	   writeLine("serverLog", "The sizes of file *FileName on different resources are not same on *ResourceGroup");	
		     	   # break from the foreach loop
			   *forloop_flag = false;
		     	   } # end of if (*new_file_size ...
			  } 
		   else{ 
		   	# the first time we record file size
			msiGetValByKey(*QueryOut, "DATA_SIZE", *file_size);
		      }
		   } # end of if (*forloop_flag ...
	      } # end of foreach (*QueryOut ...)
	      # if the sizes of files on resources are consistent, we make copies as follows
	      if (*forloop_flag){
	      	 # now for all resources that are "up" and have enough space and don't have a copy of this file
		 # we copy the file to this resource
		 *condition_q2 = "RESC_GROUP_NAME = '*ResourceGroup'";
		 msiMakeQuery("RESC_NAME", *condition_q2, *Query2);
		 msiExecStrCondQuery(*Query2, *QueryOut2);
		 # msiGetContInxFromGenQueryOut(*QueryOut2, *ContInx_q2);
		 *Qs = 0-*file_size;
		 foreach (*QueryOut2){
		 	 # we check whether the resource contains this file
			 msiGetValByKey(*QueryOut2, "RESC_NAME", *currentresourcename);
			 # check whether this resource is up and contains this file and have enough space
			 *condition_q3 = "RESC_NAME = '*currentresourcename' and DATA_NAME = *FileName and RESC_STATUS='up' and QUOTA_OVER < '*Qs'";
			 msiMakeQuery("RESC_NAME, DATA_NAME, QUOTA_OVER", *condition_q3, *Query3);
			 msiExecStrCondQuery(*Query3, *QueryOut3);
			 msiGetContInxFromGenQueryOut(*QueryOut3, *ContInx_q3);
			 if (*ContInx_q3 < 1){
			    # this means this resource does not contain the file we wanted
			    # we need copy this file to this resource
			    # this is the essential part
			    writeLine("serverLog", "The file *FileName will be replicated from cache to *currentresourcename");
			    msiGetValByKey(*query2, "COLL_NAME", *Collection);
			    *Path = "*Collection"++"/"++"*FileName";
			    if (msiDataObjReplWithOptions(*Path, *currentresourcename, "irodsAdmin", *Status)==0){
			       writeLine("serverLog", "The file *FileName has been successfully replicated");
			       writeLine("serverLog", "The file *File will be deleted from cache");
			       # delete the file from disk cache
			       msiDataObjUnlink("objPath=*Path++++replNum=*Replica++++forceFlag=", *Status);
				}
			    else {
			       writeLine("serverLog", "Failed to replicate file *FileName to *currentresourcename");
				}
			    }
		 	 }
		 # we delete the file from disk Cache 		 
		 
	      	 }
	   }
	}
	

}

INPUT *UserName="yzheng", *FileName="test_1.txt", *CollectionName="/osg/home/yzheng", *ResourceGroup = "osgSrmGroup"
OUTPUT ruleExecOut # is this correct?

