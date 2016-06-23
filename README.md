# COMET
##Synopsis

COMET is a distributed meta-data service which stores key-value oriented configuration information about resources and applications running in the distributed cloud. Clients of COMET are elements of IaaS cloud provider system, user client tools, applications running in tenant virtual systems/slices. COMET provides strong authorization controls ensuring that information is only shared with appropriate clients.  

#COMET Principals
- Users. These include tenants responsible for creating the virtual systems or slices and, also other users authorized to access individual elements of the virtual systems and configure applications on them.

- IaaS provider actors (controllers, aggregate managers)

- Provider infrastructure control entities (e.g. SDN controllers)

- Compute instances launched by users. These can use ‘speaks-for’ or have their own independently minted credentials (by e.g. the IaaS provider)

- Applications running inside compute instances. These can use speaks-for user credentials, or have purpose-made credentials minted by users and transferred securely to the applications.

###Principle of operation
Users of COMET (all principals above) can create and access scopes. Each scope contains one or more key-value pairs or a binary blob and is associated with a set of access rules. Scopes are built within contexts and have unique names significant to their creators. 

Contexts have types and are identified by a unique id. These are the types:
- Virtual System/Slice (e.g. for sharing information about the slice among principals associated with the slice, i.e. IaaS actors and tenants). Context identified by a unique slice/virtual system ID (GUID)
Two subtypes - ‘.iaas’ and ‘.user’.

- Reservation (e.g. for sharing information about the reservation among principals associated with the reservation, i.e. IaaS actors and tenants). Context identified by a unique reservation ID (GUID)
Two subtypes - ‘.iaas’ and ‘.user’

- Principal (e.g. for sharing information relevant to one or more principals - IaaS actors, tenants, compute instances and applications). Context identified e.g. by principal’s SHA-1 hash of their public key. 

An association between a principal and the context is established by creating assertions. E.g. an IaaS provider can create an assertion associating a particular tenant principal with a slice ID or a reservation ID. This is done at the time of slice/reservation creation.

Scopes contain JSON documents that encode either series of key-value pairs or a single blob of text, serializing e.g. some binary data. 

#COMET API operations

-createScope(contextType, contextID, scopeName, scopeValue) - create a named scope within a context.  Requires create access. 
-destroyScope(contextType, contextID, scopeName) - destroy scope within a context. Requires  destroy access
-readScope(contextType, contextID, scopeName) - retrieve a value from a named scope within a context. Requires read access
Can have helper methods that parse JSON to e.g. get specific values for keys
-Modify scope (require modify access)
modify a key value in a named scope within a context
-add a key/value pair in a named scope within a context
remove a key/value pair in a named scope within a context
these can be implemented as helper methods
-enumerateScopes(contextType, contextID) - return a list of existing scopes within a given context. Requires enumerate access.
##Access rules
Access rules are based on the context and the attributes of the principal. Additional rules can also be explicit to override or add to the implicit rules.

##Examples of implicit access rules:
- Principals carrying ‘iaas-actor’ attribute attested to by one of the trust roots can create, enumerate, read, modify and destroy existing scopes in slice.iaas and reservation.iaas contexts.

- Principals carrying ‘slice-owner(X)’ attribute attested to by one of the IaaS providers can enumerate scopes and read any scope within that slice.iaas context X.  Similar for ‘reservation-owner’ attribute and reservation.iaas context 

- Principals carrying ‘slice-user(X)’ attribute attested to by one of the slice owners can enumerate scopes and have read access to any scope within slice.iaas context X
Similar for reservation context, if needed (may be too much granularity)

- Principals carrying ‘slice-owner(X)’ attribute attested to by one of the IaaS providers can create, enumerate, read, modify and destroy scopes in slice.user and reservation.user contexts for indicated slices
Similar for ‘reservation-owner’ attribute and reservation.user context

- Principals carrying ‘slice-user(X)’ attribute attested to by one of the slice owners can have enumerate and read access to any scope within slice.user context X.

- Principal carrying ‘delegate-[enumerate/read/modify](X, Y)’ attribute attested to by scope owner can enumerate, read, or modify scope Y in context X. 

- Principal carrying ‘speaks-for(Z)’ attribute attested to by slice owner identified as Z can perform same operations as slice owner on any scope in any context as Z. 


##Motivation

##Installation

##API Reference

##Contributors
Most of the requirements of this service have been specified by @ibaldin
##License
/*
* Copyright (c) 2016 RENCI/UNC Chapel Hill 
*
* @author Claris Castillo
*
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
* and/or hardware specification (the "Work") to deal in the Work without restriction, including 
* without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
* sell copies of the Work, and to permit persons to whom the Work is furnished to do so, subject to 
* the following conditions:  
* The above copyright notice and this permission notice shall be included in all copies or 
* substantial portions of the Work.  
*
* THE WORK IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
* OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
* OUT OF OR IN CONNECTION WITH THE WORK OR THE USE OR OTHER DEALINGS 
* IN THE WORK.
*/
