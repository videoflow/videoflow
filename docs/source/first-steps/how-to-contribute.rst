How to contribute
=================

Found a bug? Have a new feature to suggest? Want to contribute changes to the codebase? Make sure to read this first.

Bug reporting
-------------
Your code doesn't work, and you have determined that the issue lies with Videoflow? Follow
these steps to report a bug.

1. Your bug may already be fixed.  Make sure to update to the current
Videoflow master branch.

2. Search for similar issues. Make sure to delete `is:open` on the
issue search to find solved tickets as well. It's possible somebody
has encountered this bug already.  Still having a problem? Open an issue on Github
to let us know.

3. Make sure to provide us with useful information about
your configuration: What OS are you using? What Tensorflow version are you using?
Are you running on GPU? If so, what is your version of Cuda, of CuDNN? 
What is your GPU?

4. Provide us with a script to reproduce the issue.  This script should
be runnable as-is and should not require external data download
(use randomly generated data if you need to test the flow in some data).
We recommend that you use Github Gists to post your code.
Any issue that cannot be reproduced is likely to be closed.

5. If possible, take a shot at fixing the bug yourself --if you can!

The more information you provide, the easir it is for us to validate that
there is a bug and the faster we'll be able to take action.
If you want your issue to be resolved quickly, following the steps
above is crucial.

Requesting a Feature
--------------------
You can also use Github issues to request features you would
like to see in Videoflow, or changes to the Videoflow API.

1. Provide a clear and detailed explanation of the feature
you want and why it's important to add. Keep in mind that
we want features that will be useful to the majority of our 
users and not just a small subset.  If you are targeting 
a minority of users, consider writing and add-on library
for Videoflow.

2. Provide code snippets demostrating the API you have in
mind and illustrating the use cases of your feature.

3. After discussing the feature you may choose to attempt 
a Pull Request.  If you are at all able, start writing
some code.  We always have more work to do than time to
do it.  If you can write some code then that will speed
the process along.

Pull Requests (PRs)
-------------------
**Where should I submit my pull request?** Videoflow
improvements and bug gixes should go to the Videoflow
`master` branch.

Here is a quick guide on how to submit your improvements::

1. Write the 
code.

2. Make sure any new function or class you introduce has
proper docstrings. Make sure any code you touch still
has up-to-date docstrings and documentation.  Use
previously written code as a reference on how to format
them.  In particular, they should be formatted in MarkDown,
and there should be sections for `Arguments`, `Returns` and
`Raises` (if applicable). 

3. Write tests. Your code should have full unit test coverage.
If you want to see your PRs merged promptly, this is crucial.

4. Run our test suite locally. It is easy: from the 
Videoflow folder, simply run ``py.test tests/``


5. Make sure all tests are 
passing.


6. When committing, use appropriate, descriptive 
commit messages.

7. Update the documentation.  If introducing new functionality,
make sure you include code snippets demonstrating the usage
of your new feature.

8. Submit your PR. If your changes have been approved in
a previous discussion, and if you have complete (and passing)
unit tests as well as proper doctrings/documentation, your
PR is likely to be merged promptly.

Adding new examples
-------------------
Even if you do not contribute to the Videoflow source code,
if you have an application of Videoflow as is concise and
powerful, please consider adding it to our collection of
`examples <https://github.com/jadielam/videoflow/tree/master/examples>`_.
