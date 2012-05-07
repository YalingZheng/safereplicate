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
	writeLine("stdout", "just for test");
	# looking for resources that file exist in
	*condition_q = "USER_NAME = '*UserName' and DATA_NAME = '*FileName' and COLL_NAME = '*CollectionName' and RESC_GROUP_NAME = '*ResourceGroup'";
	msiMakeQuery("RESC_NAME, DATA_SIZE", *condition_q, *Query);
	msiExecStrCondQuery(*Query, *QueryOut);
	msiGetContInxFromGenQueryOut(*QueryOut, *ContInx_q);
	writeLine("stdout", "ContInx_q = *ContInx_q");
	*file_size = 0;
	foreach (*QueryOut){
		msiGetValByKey(*QueryOut, "RESC_NAME", *currentresourcename);
		writeLine("stdout", "*currentresourcename contains *FileName");
		msiGetValByKey(*QueryOut, "DATA_SIZE", *file_size);
	}
	*Qs = 0 - int(*file_size);
	*condition_q2 = "RESC_STATUS = 'up' and RESC_GROUP_NAME = '*ResourceGroup'";
	msiMakeQuery("RESC_NAME", *condition_q2, *Query2);
	msiExecStrCondQuery(*Query2, *QueryOut2);
	msiGetContInxFromGenQueryOut(*QueryOut2, *ContInx_q2);
	writeLine("stdout", "ContInx_q2 = *ContInx_q2");
	foreach (*QueryOut2){
		msiGetValByKey(*QueryOut2, "RESC_NAME", *currentresourcename);
		writeLine("stdout", "*currentresourcename is up");
		# now, we want to check whether this resource is within quota
		*condition_q3 = "QUOTA_RESC_NAME = '*currentresourcename' and QUOTA_OVER <= '*Qs'";
		msiMakeQuery("QUOTA_RESC_NAME", *condition_q3, *Query3);
		msiExecStrCondQuery(*Query3, *QueryOut3);
		msiGetContInxFromGenQueryOut(*QueryOut3, *ContInx_q3);
		writeLine("stdout", "ContInx_q3 = *ContInx_q3");
		foreach (*QueryOut3){
			msiGetValByKey(*QueryOut3, "QUOTA_RESC_NAME", *resourcename);
			writeLine("stdout", "*resourcename has enough quota ...");
			*condition_q4 = "DATA_NAME = '*FileName' and RESC_NAME = '*resourcename'";
			msiMakeQuery("RESC_NAME", *condition_q4, *Query4);
			msiExecStrCondQuery(*Query4, *QueryOut4);
			msiGetContInxFromGenQueryOut(*QueryOut4, *ContInx_q4);
			writeLine("stdout", "ContInx_q4 = *ContInx_q4");
			*containflag = false;
			foreach (*QueryOut4){
				msiGetValByKey(*QueryOut4, "RESC_NAME", *finalresourcename);
				writeLine("stdout", "resource *finalresourcename contains *FileName");	
				*containflag = true;
			}
			if (*containflag==false){
			   writeLine("stdout", "resource *resourcename does not contain *FileName ... preparing to copy the file into this resource ...");
			   # Now, we want to copy this resource
			   *Path = "*CollectionName"++"/"++"*FileName*";
			   if (errorcode(msiDataObjReplWithOptions(*Path, *resourcename, "irodsAdmin", *Status))<0){
			      writeLine("stdout", "The file *FileName has been successfully replicated to resource *resourcename");
			   }

			}

		}
		
	}
	
}

input *UserName="yzheng", *FileName="hello3.txt", *CollectionName="/osg/home/yzheng", *ResourceGroup = "osgSrmGroup"
output ruleExecOut 

